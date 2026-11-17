"""
MetaTrack Hub v2 - program pengolah di laptop (Subbab 2.4 - 2.6 paper)

Alur:
  node 1-3 -> broker MQTT -> program ini -> hasil olahan (MQTT / Firebase opsional) -> dashboard

Yang dikerjakan:
  1. Berlangganan <PREFIX>/+/data, menyimpan semua paket ke SQLite
  2. Packet loss per node dari celah seq (restart node tidak dihitung sebagai loss)
  3. Tegangan v -> Rs (pers. 1) -> Rs/R0 -> ppm (pers. 2, setelah a dan b dikalibrasi)
  4. Alarm per node: dasar + 3 sigma selama 3 sampel berturut-turut
  5. Lokalisasi sumber:
       - pembanding 1: posisi node dengan konsentrasi tertinggi
       - pembanding 2: weighted centroid (bobot = konsentrasi di atas dasar)
       - MetaTrack: inversi Gaussian Plume (pers. 3-6) -> peta probabilitas + laju Q
       - konsensus leave-one-out (butuh >= 4 node; dengan 3 node tidak berlaku)
  6. Hasil realtime:  <PREFIX>/hasil/<node>   dan   <PREFIX>/hasil/lokasi

Perintah ke <PREFIX>/hub/cmd :
  baseline        mulai ulang perekaman nilai dasar (lakukan di UDARA BERSIH)
  label=teks      label skenario yang ikut tersimpan di database
  wind=2.5        kecepatan angin dari anemometer (m/s), dibutuhkan inversi
  source=0.30,0.20  posisi sumber sebenarnya (x,y meter) untuk menghitung galat; source=off untuk mematikan

Pasang:    pip install paho-mqtt numpy
Jalankan:  python metatrack_hub.py                (opsi: --baseline 60 --label uji1 --db metatrack.db)
Uji logika tanpa alat:  python metatrack_hub.py --selftest
"""

import argparse
import json
import math
import queue
import sqlite3
import statistics
import threading
import time
import urllib.request
from collections import deque

import numpy as np

# ============================ KONFIGURASI ============================
MQTT_HOST = "broker.emqx.io"
MQTT_PORT = 1883
MQTT_USER = "Sintia"
MQTT_PASS = "Batam2007"
PREFIX = "metatrack-2007"            # harus sama dengan di firmware dan MQTTX
NODES = ["1", "2", "3"]              # tambahkan "4" kalau Node 4 sudah jalan

VC = 5.0                             # tegangan suplai MQ-4 (V): ukur dengan multimeter
RL = 20000.0                         # resistor beban modul MQ-4 (ohm): cek nilai asli di modulmu

# Kalibrasi per node (Subbab 2.5). R0 = None -> R0 otomatis dari rata-rata Rs udara bersih (periode baseline).
# a, b = None -> ppm belum dihitung; sinyal alarm memakai indeks gas R0/Rs.
# Isi a dan b dari regresi Rs/R0 = a * C^b dengan butana acuan, terpisah untuk tiap node.
CAL = {
    "1": {"R0": None, "a": None, "b": None},
    "2": {"R0": None, "a": None, "b": None},
    "3": {"R0": None, "a": None, "b": None},
    "4": {"R0": None, "a": None, "b": None},
}

ALARM_SIGMA = 3.0                    # ambang alarm = dasar + 3 sigma
ALARM_CONSECUTIVE = 3                # selama 3 sampel berturut-turut

# ---- Geometri dan model Gaussian Plume (Subbab 2.3 dan 2.6) ----
# Titik acuan (0,0) = sudut hulu pelat, angin searah sumbu +x. Satuan meter.
# !! NODE_XY di bawah ini PLACEHOLDER. Ukur posisi tiap node di pelatmu (Gambar 4) lalu isi. !!
NODE_XY = {
    "1": (0.50, 0.15),
    "2": (0.50, 0.45),
    "3": (1.00, 0.30),
    "4": (1.00, 0.15),
}
PLATE_L, PLATE_W = 1.5, 0.6          # pelat baja 1,5 m x 0,6 m
GRID_STEP = 0.02                     # kandidat sumber tiap 2 cm
SENSOR_Z = 0.10                      # tinggi sensor (m)
SOURCE_H = 0.10                      # tinggi sumber (m): ukur ujung selang
# sigma_y = a * x^b dan sigma_z = a * x^b (x = jarak searah angin, m).
# !! PLACEHOLDER. Cocokkan a dan b pada pelepasan kalibrasi yang posisi sumbernya diketahui (Subbab 2.6). !!
SIGMA_Y = (0.20, 0.9)
SIGMA_Z = (0.08, 0.9)
MIN_DX = 0.02                        # node harus minimal 2 cm di hilir kandidat
PPM_TO_GM3 = 2.377e-3                # 1 ppm butana -> g/m3 (M 58,12 g/mol, 25 C, 1 atm)
SIGMA_N_MIN_GM3 = 0.005              # lantai simpangan baku derau (g/m3)
SIGMA_N_REL = 0.10                   # sigma_n juga minimal 10% dari konsentrasi tertinggi (galat model plume);
                                     # ganti setelah kalibrasi (paper: sigma_n dari kalibrasi)
TAU = 3.0                            # ambang konsensus (simpangan baku derau)
KONSENSUS_AKTIF = False              # leave-one-out (paper). EKSPERIMENTAL: pada uji sintetis dengan 4 node,
                                     # metode ini bisa menandai node sehat. Nyalakan hanya untuk dievaluasi.

MA_N = 5                             # rerata bergerak (jumlah sampel) untuk lokalisasi
STALE_S = 10                         # node tanpa data > 10 detik dianggap tidak aktif
LOC_PERIOD_S = 2.0                   # lokalisasi dihitung maksimal tiap 2 detik
MAP_CELL = 0.05                      # sel peta probabilitas untuk dashboard (5 cm)

# Firebase Realtime Database (opsional, kosongkan untuk mematikan)
# Contoh: "https://NAMA-PROYEKMU-default-rtdb.firebaseio.com"
# PERHATIAN: aturan database harus dibatasi; jangan biarkan terbuka untuk semua orang.
FIREBASE_URL = ""
# =====================================================================

GX, GY = np.meshgrid(
    np.arange(0.0, PLATE_L + 1e-9, GRID_STEP),
    np.arange(0.0, PLATE_W + 1e-9, GRID_STEP),
)


# ------------------------- model Gaussian Plume -------------------------
def gain(xs, ys, xi, yi, u):
    """g_i = C/Q untuk sumber di (xs, ys) dan node di (xi, yi). xs, ys boleh array (broadcast)."""
    dx = xi - xs
    ok = dx > MIN_DX
    dxs = np.where(ok, dx, 1.0)
    sy = SIGMA_Y[0] * dxs ** SIGMA_Y[1]
    sz = SIGMA_Z[0] * dxs ** SIGMA_Z[1]
    dy = yi - ys
    g = (1.0 / (2.0 * math.pi * u * sy * sz)) * np.exp(-dy ** 2 / (2.0 * sy ** 2)) * (
        np.exp(-((SENSOR_Z - SOURCE_H) ** 2) / (2.0 * sz ** 2))
        + np.exp(-((SENSOR_Z + SOURCE_H) ** 2) / (2.0 * sz ** 2))
    )
    return np.where(ok, g, 0.0)


def invert(xy, c, u, sigma_n):
    """Pers. 4-6: Q terbaik tiap kandidat (kuadrat terkecil), residual J, peta probabilitas P."""
    n = len(c)
    G = np.stack([gain(GX, GY, xy[i][0], xy[i][1], u) for i in range(n)])
    c3 = np.asarray(c, dtype=float)[:, None, None]
    num = (c3 * G).sum(0)
    den = (G ** 2).sum(0)
    Q = np.where(den > 1e-30, num / np.where(den > 1e-30, den, 1.0), 0.0)
    Q = np.clip(Q, 0.0, None)
    J = ((c3 - Q * G) ** 2).sum(0)
    logp = -J / (2.0 * sigma_n ** 2)
    logp -= logp.max()
    P = np.exp(logp)
    P /= P.sum()
    k = np.unravel_index(np.argmax(P), P.shape)
    return {"x": float(GX[k]), "y": float(GY[k]), "Q": float(Q[k]), "P": P, "conf": float(P[k])}


def estimate(ids, c_gm3, u, sigma_n, node_xy=None):
    """Inversi + konsensus leave-one-out (bila >= 4 node). c_gm3 = konsentrasi di atas dasar (g/m3)."""
    node_xy = node_xy or NODE_XY
    xy = [node_xy[i] for i in ids]
    c = np.asarray(c_gm3, dtype=float)
    n = len(ids)
    flagged = []
    keep = list(range(n))
    # leave-one-out: node dengan residual terbesar dikeluarkan satu per satu (supaya satu node
    # yang rusak tidak membuat node sehat ikut tertandai), selama masih tersisa >= 3 node
    while KONSENSUS_AKTIF and len(keep) >= 4:
        zs = {}
        for i in keep:
            others = [j for j in keep if j != i]
            r = invert([xy[j] for j in others], c[others], u, sigma_n)
            pred = r["Q"] * float(gain(r["x"], r["y"], xy[i][0], xy[i][1], u))
            zs[i] = abs((c[i] - pred) / sigma_n)
        worst = max(zs, key=zs.get)
        if zs[worst] <= TAU:
            break
        flagged.append(ids[worst])
        keep.remove(worst)
    res = invert([xy[j] for j in keep], c[keep], u, sigma_n)
    res["flagged"] = flagged
    res["n_used"] = len(keep)
    res["consensus"] = KONSENSUS_AKTIF and n >= 4
    return res


def coarse_map(P):
    """Peta probabilitas kasar (sel 5 cm) untuk dashboard: baris y, kolom x."""
    nx, ny = int(round(PLATE_L / MAP_CELL)), int(round(PLATE_W / MAP_CELL))
    cx = np.minimum((GX / MAP_CELL).astype(int), nx - 1)
    cy = np.minimum((GY / MAP_CELL).astype(int), ny - 1)
    m = np.bincount((cy * nx + cx).ravel(), weights=P.ravel(), minlength=nx * ny)
    return {"cell_m": MAP_CELL, "nx": nx, "ny": ny, "p": [round(float(v), 4) for v in m]}


# ------------------------------ sensor MQ-4 ------------------------------
def compute_rs(v):
    """Pers. 1: Rs = RL * (Vc - VRL) / VRL, VRL = tegangan AO (sudah dikoreksi pembagi)."""
    if v is None or v <= 0.01 or v >= VC - 0.01:
        return None
    return RL * (VC - v) / v


def compute_ppm(node, rs, r0):
    """Pers. 2: Rs/R0 = a * C^b  ->  C = (Rs/R0 / a)^(1/b)."""
    a, b = CAL[node]["a"], CAL[node]["b"]
    if not a or not b or not r0 or not rs:
        return None
    ratio = rs / r0
    if ratio <= 0:
        return None
    try:
        return (ratio / a) ** (1.0 / b)
    except (ValueError, ZeroDivisionError, OverflowError):
        return None


def signal_of(node, rs, r0):
    """Sinyal alarm: ppm bila terkalibrasi, kalau belum pakai indeks gas R0/Rs."""
    ppm = compute_ppm(node, rs, r0)
    if ppm is not None:
        return ppm, ppm
    if rs and r0:
        return None, r0 / rs
    return None, None


class NodeState:
    def __init__(self, node):
        self.node = node
        self.first_seq = self.last_seq = None
        self.tot_expected = self.tot_received = self.cur_received = 0
        self.restarts = 0
        self.baseline_rs, self.baseline_sig = [], []
        self.base_mean = self.base_std = None
        self.r0 = CAL[node]["R0"]
        self.over_count = 0
        self.alarm = False
        self.ex_ma = deque(maxlen=MA_N)
        self.last_seen = 0.0

    def register_seq(self, seq):
        if self.last_seq is None:
            self.first_seq = self.last_seq = seq
            self.cur_received = 1
            return True
        if seq > self.last_seq:
            self.cur_received += 1
            self.last_seq = seq
            return True
        if seq == self.last_seq:
            return False
        # seq mundur: node restart
        self.tot_expected += self.last_seq - self.first_seq + 1
        self.tot_received += self.cur_received
        self.first_seq = self.last_seq = seq
        self.cur_received = 1
        self.restarts += 1
        return True

    def loss_pct(self):
        exp = self.tot_expected + (self.last_seq - self.first_seq + 1 if self.last_seq is not None else 0)
        rec = self.tot_received + self.cur_received
        return round((exp - rec) / exp * 100.0, 2) if exp > 0 else 0.0


class Store:
    def __init__(self, path):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node TEXT, seq INTEGER, t_ms INTEGER, recv_ms INTEGER, latency_ms INTEGER,
                v REAL, rs REAL, ppm REAL, signal REAL, alarm INTEGER,
                temp REAL, hum REAL, label TEXT)"""
        )
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                t_ms INTEGER, n_nodes INTEGER, status TEXT,
                x_est REAL, y_est REAL, q_est REAL, conf REAL,
                cx REAL, cy REAL, maxnode TEXT, flagged TEXT,
                x_true REAL, y_true REAL, err_pct REAL, label TEXT)"""
        )
        self.db.commit()

    def add(self, row):
        with self.lock:
            self.db.execute(
                """INSERT INTO readings
                   (node, seq, t_ms, recv_ms, latency_ms, v, rs, ppm, signal, alarm, temp, hum, label)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""", row)
            self.db.commit()

    def add_loc(self, row):
        with self.lock:
            self.db.execute(
                """INSERT INTO locations
                   (t_ms, n_nodes, status, x_est, y_est, q_est, conf, cx, cy, maxnode, flagged,
                    x_true, y_true, err_pct, label)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", row)
            self.db.commit()


# ------------------------------- hub utama -------------------------------
class Hub:
    def __init__(self, store, emit, baseline_n=60, label=""):
        self.store, self.emit = store, emit
        self.ctx = {"label": label, "baseline_n": baseline_n, "wind": None, "source": None}
        self.states = {n: NodeState(n) for n in NODES}
        self.last_loc = 0.0
        self.now = time.time
        self.start_baseline()

    # --- perintah ---
    def start_baseline(self):
        for st in self.states.values():
            st.baseline_rs.clear()
            st.baseline_sig.clear()
            st.base_mean = st.base_std = None
            st.over_count, st.alarm = 0, False
            st.ex_ma.clear()
            if CAL[st.node]["R0"] is None:
                st.r0 = None
        print(f"== Perekaman nilai dasar dimulai ({self.ctx['baseline_n']} sampel/node). Pastikan UDARA BERSIH. ==")

    def handle_cmd(self, text):
        text = text.strip()
        if text == "baseline":
            self.start_baseline()
        elif text.startswith("label="):
            self.ctx["label"] = text[6:]
            print("Label skenario:", self.ctx["label"])
        elif text.startswith("wind="):
            try:
                self.ctx["wind"] = float(text[5:])
                print("Kecepatan angin:", self.ctx["wind"], "m/s")
            except ValueError:
                print("Format wind salah, contoh: wind=2.5")
        elif text.startswith("source="):
            val = text[7:].strip()
            if val == "off":
                self.ctx["source"] = None
            else:
                try:
                    x, y = [float(p) for p in val.split(",")]
                    self.ctx["source"] = (x, y)
                    print("Posisi sumber sebenarnya:", self.ctx["source"])
                except ValueError:
                    print("Format source salah, contoh: source=0.30,0.20")

    # --- data node ---
    def handle_data(self, raw, recv_ms):
        try:
            d = json.loads(raw)
            node, seq, t_ms, v = str(d["node"]), int(d["seq"]), int(d["t_ms"]), float(d["v"])
        except (ValueError, KeyError, TypeError):
            print("Paket tidak valid diabaikan:", raw[:80])
            return
        st = self.states.get(node)
        if st is None or not st.register_seq(seq):
            return
        temp, hum = d.get("t"), d.get("h")
        rs = compute_rs(v)

        in_baseline = st.base_mean is None
        if in_baseline:
            if rs is not None:
                st.baseline_rs.append(rs)
            if len(st.baseline_rs) >= self.ctx["baseline_n"]:
                if st.r0 is None:
                    st.r0 = statistics.mean(st.baseline_rs)
                    print(f"Node {node}: R0 otomatis = {st.r0:.0f} ohm")
                sigs = [signal_of(node, r, st.r0)[1] for r in st.baseline_rs]
                st.base_mean = statistics.mean(sigs)
                st.base_std = max(statistics.pstdev(sigs), abs(st.base_mean) * 0.01, 1e-9)
                print(f"Node {node}: nilai dasar = {st.base_mean:.4f}, sigma = {st.base_std:.4f}")
                in_baseline = False

        ppm = sig = None
        if st.r0 is not None and rs is not None:
            ppm, sig = signal_of(node, rs, st.r0)
        if st.base_mean is not None and sig is not None:
            st.over_count = st.over_count + 1 if sig > st.base_mean + ALARM_SIGMA * st.base_std else 0
            st.alarm = st.over_count >= ALARM_CONSECUTIVE
            st.ex_ma.append(max(sig - st.base_mean, 0.0))
            st.last_seen = self.now()
        else:
            st.alarm = False

        latency = recv_ms - t_ms
        self.store.add((node, seq, t_ms, recv_ms, latency, v, rs, ppm, sig, int(st.alarm), temp, hum, self.ctx["label"]))
        out = {
            "node": node, "seq": seq, "t_ms": t_ms, "recv_ms": recv_ms, "latency_ms": latency,
            "v": round(v, 4), "rs": round(rs, 1) if rs else None,
            "ppm": round(ppm, 1) if ppm is not None else None,
            "signal": round(sig, 4) if sig is not None else None,
            "baseline": bool(in_baseline), "alarm": bool(st.alarm),
            "loss_pct": st.loss_pct(), "restarts": st.restarts, "label": self.ctx["label"],
        }
        if temp is not None:
            out["temp"], out["hum"] = temp, hum
        self.emit(node, out)
        print(f"[{node}] seq={seq} v={v:.3f} ppm={out['ppm']} sinyal={out['signal']} "
              f"alarm={'YA' if st.alarm else '-'} loss={out['loss_pct']}% lat={latency}ms"
              + (" (baseline)" if in_baseline else ""))

        if self.now() - self.last_loc >= LOC_PERIOD_S:
            self.last_loc = self.now()
            self.localize()

    # --- lokalisasi ---
    def localize(self):
        now = self.now()
        active = [n for n in NODES if self.states[n].base_mean is not None
                  and self.states[n].ex_ma and now - self.states[n].last_seen <= STALE_S]
        out = {"t_ms": int(now * 1000), "n_nodes": len(active), "nodes": active, "label": self.ctx["label"]}
        if len(active) < 2:
            out["status"] = "menunggu_data_node"
            self.emit("lokasi", out)
            return out
        alarm_nodes = [n for n in active if self.states[n].alarm]
        out["alarm_nodes"] = alarm_nodes
        out["status"] = "bocor" if alarm_nodes else "normal"
        if not alarm_nodes:
            self.emit("lokasi", out)
            return out

        exc = {n: statistics.mean(self.states[n].ex_ma) for n in active}
        ppm_mode = all(CAL[n]["a"] and CAL[n]["b"] for n in active)
        out["unit"] = "ppm" if ppm_mode else "indeks"
        out["excess"] = {n: round(v, 4) for n, v in exc.items()}

        # pembanding 1 dan 2 (selalu tersedia)
        top = max(active, key=exc.get)
        out["max_node"] = {"node": top, "x": NODE_XY[top][0], "y": NODE_XY[top][1]}
        wsum = sum(exc.values())
        if wsum > 0:
            out["centroid"] = {"x": round(sum(exc[n] * NODE_XY[n][0] for n in active) / wsum, 3),
                               "y": round(sum(exc[n] * NODE_XY[n][1] for n in active) / wsum, 3)}

        # MetaTrack: inversi Gaussian Plume
        est = None
        if len(active) < 3:
            out["inversi"] = "butuh_minimal_3_node"
        elif not ppm_mode:
            out["inversi"] = "menunggu_kalibrasi_ab"
        elif not self.ctx["wind"]:
            out["inversi"] = "menunggu_kecepatan_angin"
        else:
            sig_n = statistics.mean(self.states[n].base_std for n in active) * PPM_TO_GM3
            c = [exc[n] * PPM_TO_GM3 for n in active]
            sig_n = max(sig_n, SIGMA_N_MIN_GM3, SIGMA_N_REL * max(c))
            est = estimate(active, c, self.ctx["wind"], sig_n)
            out["inversi"] = "ok"
            out["metatrack"] = {"x": round(est["x"], 3), "y": round(est["y"], 3),
                                "q_gps": round(est["Q"], 6), "conf": round(est["conf"], 4),
                                "n_used": est["n_used"], "flagged": est["flagged"],
                                "konsensus": ("aktif" if est["consensus"] else
                                              ("nonaktif" if len(active) >= 4 else "tidak_berlaku_kurang_dari_4_node"))}
            out["map"] = coarse_map(est["P"])

        src = self.ctx["source"]
        if src:
            def err(p):
                return round(math.hypot(p["x"] - src[0], p["y"] - src[1]) / math.hypot(PLATE_L, PLATE_W) * 100, 2)
            out["galat_pct_diagonal"] = {"max_node": err(out["max_node"])}
            if "centroid" in out:
                out["galat_pct_diagonal"]["centroid"] = err(out["centroid"])
            if est:
                out["galat_pct_diagonal"]["metatrack"] = err(out["metatrack"])

        m, c0 = out.get("metatrack"), out.get("centroid")
        self.store.add_loc((out["t_ms"], len(active), out["status"],
                            m["x"] if m else None, m["y"] if m else None, m["q_gps"] if m else None,
                            m["conf"] if m else None, c0["x"] if c0 else None, c0["y"] if c0 else None,
                            top, ",".join(est["flagged"]) if est else None,
                            src[0] if src else None, src[1] if src else None,
                            out.get("galat_pct_diagonal", {}).get("metatrack"), self.ctx["label"]))
        self.emit("lokasi", out)
        print("LOKASI", {k: out[k] for k in ("status", "max_node", "centroid", "metatrack") if k in out})
        return out


# ------------------------------ Firebase ------------------------------
fb_queue = queue.Queue(maxsize=200)


def firebase_worker():
    while True:
        key, payload = fb_queue.get()
        try:
            req = urllib.request.Request(f"{FIREBASE_URL}/latest/{key}.json",
                                         data=json.dumps(payload).encode(), method="PUT",
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=3).read()
        except Exception as e:
            print("Firebase gagal:", e)


# ------------------------------- uji logika -------------------------------
def selftest():
    """Uji tanpa alat: plume sintetis -> pipeline lengkap. Parameter model di sini = parameter hub."""
    rng = np.random.default_rng(1)
    u, src, Qtrue = 2.0, (0.15, 0.30), 0.01
    print("== Uji 1: inversi 3 node dan 4 node (data sintetis, derau 2%) ==")
    for ids in (["1", "2", "3"], ["1", "2", "3", "4"]):
        c = [Qtrue * float(gain(src[0], src[1], *NODE_XY[i], u)) for i in ids]
        c = [v * (1 + 0.02 * rng.standard_normal()) for v in c]
        r = estimate(ids, c, u, max(SIGMA_N_MIN_GM3, SIGMA_N_REL * max(c)))
        e = math.hypot(r["x"] - src[0], r["y"] - src[1]) / math.hypot(PLATE_L, PLATE_W) * 100
        print(f"  {len(ids)} node: estimasi ({r['x']:.2f},{r['y']:.2f}) Q={r['Q']:.4f} (benar Q={Qtrue}) galat={e:.2f}% D")

    print("== Uji 2: konsensus leave-one-out (EKSPERIMENTAL, hanya berlaku >= 4 node), Node 4 diberi bias x3 ==")
    global KONSENSUS_AKTIF
    KONSENSUS_AKTIF = True
    ids = ["1", "2", "3", "4"]
    c = [Qtrue * float(gain(src[0], src[1], *NODE_XY[i], u)) for i in ids]
    c[3] *= 3.0
    r = estimate(ids, c, u, max(SIGMA_N_MIN_GM3, SIGMA_N_REL * max(c)))
    print(f"  ditandai menyimpang: {r['flagged']} (yang dibias: ['4']) | dipakai {r['n_used']} node | estimasi ({r['x']:.2f},{r['y']:.2f})")
    print("  catatan: dengan 4 node, sisa 3 node pas dengan 3 yang tak diketahui (x, y, Q), jadi daya deteksi node rusak terbatas.")
    KONSENSUS_AKTIF = False

    print("== Uji 3: pipeline penuh (baseline -> kebocoran -> alarm -> lokasi), node 1-3 ==")
    for n in NODES:
        CAL[n].update({"a": 2.78, "b": -0.226})
    clock = {"t": 1_000_000.0}
    got = []
    hub = Hub(Store(":memory:"), lambda k, p: got.append((k, p)), baseline_n=20, label="selftest")
    hub.now = lambda: clock["t"]
    hub.handle_cmd("wind=2.0")
    hub.handle_cmd(f"source={src[0]},{src[1]}")
    R0 = 20000.0
    base_ppm = (1.0 / 2.78) ** (1.0 / -0.226)

    def v_of(c_total):
        rs = R0 * 2.78 * c_total ** -0.226
        return VC * RL / (rs + RL)

    seq = {n: 0 for n in NODES}

    def step(excess_ppm):
        clock["t"] += 2.0
        for n in NODES:
            ct = base_ppm + excess_ppm.get(n, 0.0)
            v = v_of(ct) * (1 + 0.002 * rng.standard_normal())
            pkt = json.dumps({"node": n, "seq": seq[n], "t_ms": int(clock["t"] * 1000) - 30, "v": round(v, 4)})
            seq[n] += 1
            hub.handle_data(pkt, int(clock["t"] * 1000))

    import io, contextlib
    with contextlib.redirect_stdout(io.StringIO()):
        for _ in range(25):
            step({})
        leak = {n: Qtrue * float(gain(src[0], src[1], *NODE_XY[n], u)) / PPM_TO_GM3 for n in NODES}
        for _ in range(12):
            step(leak)
    last = [p for k, p in got if k == "lokasi" and p.get("status") == "bocor"][-1]
    print("  alarm node:", last["alarm_nodes"], "| centroid:", last.get("centroid"), "| MetaTrack:", last.get("metatrack"))
    print("  galat % diagonal:", last.get("galat_pct_diagonal"))
    print("  packet loss:", {k: p["loss_pct"] for k, p in got if k in NODES}.get("3"))


# ------------------------------ program utama ------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", type=int, default=60, help="sampel udara bersih per node (60 = ~2 menit)")
    ap.add_argument("--label", default="")
    ap.add_argument("--db", default="metatrack.db")
    ap.add_argument("--selftest", action="store_true", help="uji logika dengan data sintetis lalu keluar")
    args = ap.parse_args()
    if args.selftest:
        selftest()
        return

    import paho.mqtt.client as mqtt

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="metatrack-hub")
    except AttributeError:                       # paho-mqtt 1.x
        client = mqtt.Client(client_id="metatrack-hub")
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.will_set(f"{PREFIX}/hub/status", "offline", retain=True)

    if FIREBASE_URL:
        threading.Thread(target=firebase_worker, daemon=True).start()

    def emit(key, payload):
        client.publish(f"{PREFIX}/hasil/{key}", json.dumps(payload))
        if FIREBASE_URL and not fb_queue.full():
            fb_queue.put((key, payload))

    hub = Hub(Store(args.db), emit, baseline_n=args.baseline, label=args.label)

    def on_connect(c, userdata, flags, rc, properties=None):
        ok = (rc == 0) or (hasattr(rc, "is_failure") and not rc.is_failure)
        print("MQTT tersambung" if ok else f"MQTT gagal, kode={rc}")
        c.subscribe(f"{PREFIX}/+/data")
        c.subscribe(f"{PREFIX}/hub/cmd")
        c.publish(f"{PREFIX}/hub/status", "online", retain=True)

    def on_message(c, userdata, msg):
        recv_ms = int(time.time() * 1000)
        if msg.topic == f"{PREFIX}/hub/cmd":
            hub.handle_cmd(msg.payload.decode(errors="ignore"))
        else:
            hub.handle_data(msg.payload.decode(errors="ignore"), recv_ms)

    client.on_connect, client.on_message = on_connect, on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nBerhenti.")
        client.publish(f"{PREFIX}/hub/status", "offline", retain=True)
        client.disconnect()


if __name__ == "__main__":
    main()
