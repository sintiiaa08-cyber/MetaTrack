# MetaTrack

**Low-cost wireless sensor network that locates invisible gas leaks in real time using Gaussian Plume inversion.**

MetaTrack uses 4 ESP32-based nodes with MQ-4 gas sensors and one anemometer. Each node sends a reading every 2 s over MQTT to a Python edge hub. The hub converts the sensor voltage into concentration, raises a three-sigma alarm, and estimates the leak source with three methods on the same data:

1. **Highest node:** the node with the largest excess signal
2. **Weighted centroid:** the centroid of node positions weighted by excess signal
3. **GPM inversion:** a Gaussian Plume Model inversion that returns a source probability map, the emission rate, and a confidence value

> Status: design and planned laboratory validation (butane as surrogate gas). No measured performance is claimed.

## Repository Contents

| File | Description |
|---|---|
| `metatrack_hub.py` | Edge hub: MQTT subscriber, voltage-to-ppm conversion, alarm, localization, SQLite logging |
| `metatrack.db` | SQLite database where the hub stores received packets and results |
| `firmware/` | ESP32 / ESP32-S3 node firmware [CEK: upload this folder or remove this row] |

## What You Need

**Hardware**

| Item | Qty | Note |
|---|---|---|
| ESP32 DevKit V1 | 3 | Nodes 1 to 3 |
| ESP32-S3 DevKit | 1 | Node 4 (with DHT22) |
| MQ-4 gas sensor module | 4 | One per node, load resistor 20 kOhm |
| Resistors 20 kOhm and 10 kOhm | 4 pairs | Voltage divider (ratio 1/3), keeps the 5 V output within the 3.3 V ADC limit |
| DHT22 | 1 | Temperature and humidity, logged only |
| Anemometer | 1 | Wind speed as model input |
| Fan + honeycomb straightener | 1 | Uniform wind along +x |
| Butane cylinder + needle valve | 1 | Surrogate gas for lab tests |
| 5 V power supply per node | 4 | MQ-4 heater needs 5 V |
| Laptop or single-board computer | 1 | Runs the hub and MQTT broker |

**Software**

- Python 3
- Python libraries: `paho-mqtt`, `numpy` (SQLite is built into Python)
- An MQTT 3.1.1 broker, for example Mosquitto
- Arduino IDE or PlatformIO for the node firmware

## Wiring (per node)

```
MQ-4 AO --> 20k --+--> ADC1 pin (GPIO34 on ESP32, GPIO1 on ESP32-S3)
                  |
                 10k
                  |
                 GND
MQ-4 VCC --> 5 V,  MQ-4 GND --> GND
DHT22 (Node 4 only): 3.3 V, GND, DATA --> GPIO14
```

Use ADC1 pins only, because ADC2 conflicts with Wi-Fi.

## Quick Start

1. **Start an MQTT broker** on the laptop (e.g. `mosquitto`).
2. **Flash the nodes.** Open the firmware, set your Wi-Fi name and password, the broker IP address, and the node ID, then upload it to each ESP32. [CEK: file and variable names]
3. **Install the hub dependencies:**
   ```bash
   pip install paho-mqtt numpy
   ```
4. **Run the hub:**
   ```bash
   python metatrack_hub.py
   ```
   [CEK: add arguments here if the script takes any, e.g. broker address]
5. **Warm up the sensors.** The MQ-4 needs a burn-in of about 24 h before first use.
6. **Record the clean-air baseline.** With no gas present, the hub records 60 samples per node (about 2 min).
7. **Run a test.** Switch on the fan, wait for a stable wind speed, then release butane through the needle valve. The hub raises an alarm and starts localizing.

## MQTT Topics and Commands

Nodes publish a JSON packet every 2 s to:

```
<prefix>/<node>/data
```

Each packet carries the node ID, a sequence number, an NTP time stamp, and the voltage. Node 4 also sends temperature and humidity.

Control the hub by publishing to `hub/cmd`:

| Command | Function |
|---|---|
| `baseline` | Restart the clean-air baseline recording |
| `wind` | Set the wind speed (m/s) from the anemometer |
| `label` | Set the scenario label for the current run |
| `source` | Set the true source position, so the localization error is computed automatically |

Example with Mosquitto: [CEK: replace with the exact message format your hub expects]
```bash
mosquitto_pub -t hub/cmd -m "baseline"
```

## Calibration

Each MQ-4 is calibrated separately against butane reference concentrations (250 to 2000 ppm). Fit `a` and `b` in `Rs/R0 = a * C^b` and enter them in the hub. Until they exist, the hub uses the gas index `R0/Rs` for the alarm, and the GPM inversion does not run.

## Safety

- Use butane, not methane, for all tests.
- Work in a ventilated room away from ignition sources.
- Keep the peak concentration below 20% of the lower flammable limit (about 2800 ppm).

## Citation

If you use this work, please cite the MetaTrack paper. [CEK: add title, authors, and year when published]

## Authors

Aulia Putri Herawati, Ratu Gendis Aprila, and Sintia Ompusunggu
Department of Instrumentation Engineering, Institut Teknologi Sepuluh Nopember, Surabaya, Indonesia
