# 📡 Anomaly Parser for Agentic AI-Enabled RAN

This parser is a companion service for the [`agentic-ai-enabled-ran`](https://github.com/<your-org>/agentic-ai-enabled-ran) project.  
It ingests raw RAN AI event data and produces structured **anomaly records** (JSON & CSV) that can be consumed by dashboards, self-healing pipelines, or other downstream systems.

---

## ✨ Features
- Designed specifically for **RAN AI anomaly output** from `agentic-ai-enabled-ran`.
- Extracts key anomaly types:
  - **Throughput Drop**
  - **Low RSRP**
  - **UEs Spike/Drop**
  - **Low SINR**
- Detects and attaches **Recommended Fix / Remediation** lines to anomalies.
- Supports multiple input formats:
  - Array of JSON objects
  - Single JSON object
  - NDJSON (newline-delimited JSON)
- Outputs:
  - `anomalies.json` — structured anomaly list
  - `anomalies.csv` — tabular format for quick inspection
- Containerized for easy deployment on **OpenShift** alongside the RAN AI demo.

---

## 📂 Project Structure
```
.
├── parser.py           # Entry point: loads RAN AI events, runs parse_anomalies, writes outputs
├── parse_anomalies.py  # Core regex-based anomaly parser for RAN AI logs
├── Containerfile       # Container build definition for OpenShift/Podman
└── README.md           # This file
```

---

## 📥 Input Format
The parser expects JSON objects produced by the RAN AI pipeline (from `agentic-ai-enabled-ran`).  
Each object should include an `event` (or `Event`) field containing the raw anomaly text. 

Example inputs are in the `examples/input.json`

These metadata fields are preserved in the output.

Example:
```json
[
  {
    "id": "123",
    "creation_date": "2025-04-23T21:12:52",
    "event": "Cell ID 101, Band 66:\n- Low RSRP: -104 dBm\n- Recommended fix: Adjust antenna tilt"
  }
]
```

---

## 📤 Output

An example output is in the `examples/output.json`

### JSON (`anomalies.json`)
```json
[
  {
    "cell_id": 101,
    "band": 66,
    "anomaly_type": "Low RSRP",
    "anomaly": "Low RSRP: -104 dBm",
    "recommended_fix": "Adjust antenna tilt",
    "source_id": "123",
    "creation_date": "2025-04-23T21:12:52"
  }
]
```

### CSV (`anomalies.csv`)
| cell_id | band | anomaly_type | anomaly           | recommended_fix    | source_id | creation_date       |
|---------|------|--------------|-------------------|--------------------|-----------|---------------------|
| 101     | 66   | Low RSRP     | Low RSRP: -104 dBm| Adjust antenna tilt| 123       | 2025-04-23T21:12:52 |

---

## 🚀 Running on OpenShift

To deploy on OpenShift, make sure the AI RAN demo is running.

Open manifest/deployment.yml and ensure that `RAW_URL` is pointing at the AI RAN event api.

Then, apply the manifests to your openshift cluster.

`oc apply -f manifest/deployment.yml manifest/service.yml manifest/route.yml`

### Accessing the Parser

You should be able to see the running parser using the route.

`http://<route-host>`

If the connection was made, there should be two files with the parsed anomalies

- anomalies.json
- anomalies.csv


---
