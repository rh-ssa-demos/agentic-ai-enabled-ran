import sys, json, csv
from typing import Any, Dict, List
from parse_anomalies import parse_anomalies

def load_items(path: str) -> List[Dict[str, Any]]:
    """Supports: array of objects, single object, or NDJSON."""
    with open(path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            f.seek(0)
            return [json.loads(line) for line in f if line.strip()]
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    raise TypeError(f"Unsupported top-level JSON type: {type(data)}")

def flatten(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in items:
        event_text = it.get("event") or it.get("Event")
        if not isinstance(event_text, str):
            continue
        parsed = parse_anomalies(event_text)
        # Keep useful metadata
        for r in parsed:
            if "id" in it: r["source_id"] = it["id"]
            if "creation_date" in it: r["creation_date"] = it["creation_date"]
        rows.extend(parsed)
    return rows

if __name__ == "__main__":
    in_path  = sys.argv[1] if len(sys.argv) > 1 else "payload.json"
    out_json = sys.argv[2] if len(sys.argv) > 2 else "anomalies.json"
    out_csv  = sys.argv[3] if len(sys.argv) > 3 else "anomalies.csv"

    items = load_items(in_path)
    rows = flatten(items)

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)

    # Optional CSV for quick checks
    fieldnames = ["cell_id","band","anomaly_type","anomaly","recommended_fix","source_id","creation_date"]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

    print(f"Wrote {len(rows)} anomalies → {out_json} & {out_csv}")
