import re
from typing import List, Dict

HEADER_MAIN   = re.compile(r'^Cell ID\s+(\d+),\s*Band(?:\s+Band)?\s+(\d+)\s*(?:\((?:FORMAT ERROR)\))?:\s*$', re.I)
HEADER_INLINE = re.compile(r'^\s*\d+\.\s*Cell ID\s+(\d+),\s*Band\s+(\d+)\s*$', re.I)

ANOMALY_KEYS = [
    (re.compile(r'Throughput Drop:\s*(.+)', re.I), "Throughput Drop"),
    (re.compile(r'Low RSRP:\s*(.+)', re.I),        "Low RSRP"),
    (re.compile(r'UEs?\s+Spike/Drop:\s*(.+)', re.I), "UEs Spike/Drop"),
    (re.compile(r'Low SINR:\s*(.+)', re.I),        "Low SINR"),
]

INLINE_ERROR = re.compile(
    r'LLM_FORMAT_ERROR.*?:.*?'
    r'(Throughput Drop:|Low RSRP:|UEs?\s+Spike/Drop:|Low SINR:)\s*(.+)$',
    re.I
)

# “Recommended fix” / “Remediation” lines
RECOMMENDED_FIX = re.compile(r'^(?:[-\*\u2022]\s*)?(?:Recommended\s*fix)\s*:\s*(.+)$', re.I)
REMEDIATION     = re.compile(r'^(?:[-\*\u2022]\s*)?(?:Remediation)\s*:\s*(.+)$', re.I)

def _clean(s: str) -> str:
    s = re.sub(r'^[\s•\-\*\u2022]+', '', s)
    s = re.sub(r'\s+LLM failed to format:.*$', '', s, flags=re.I)
    s = re.sub(r'\s{2,}', ' ', s).strip()
    s = re.sub(r'[.,;:]\s*$', '', s)
    return s

def parse_anomalies(event_text: str) -> List[Dict]:
    lines = [l.rstrip() for l in event_text.splitlines()]
    out: List[Dict] = []
    current = None  # (cell_id, band)
    current_block_indices: list[int] = []  # indices in `out` for the current (cell, band)

    def push(cell_id: int, band: int, typ: str, text: str):
        out.append({
            "cell_id": int(cell_id),
            "band": int(band),
            "anomaly_type": typ,
            "anomaly": f"{typ}: {_clean(text)}",
        })
        current_block_indices.append(len(out) - 1)

    for raw in lines:
        line = raw.strip()

        # New section header → reset the block
        m = HEADER_MAIN.match(line) or HEADER_INLINE.match(line)
        if m:
            current = (int(m.group(1)), int(m.group(2)))
            current_block_indices = []
            continue
        if not current:
            continue

        # Apply “Recommended fix” to all anomalies of the current block
        rf = RECOMMENDED_FIX.search(line)
        if rf:
            fix = _clean(rf.group(1))
            for i in current_block_indices:
                out[i]["recommended_fix"] = fix
            continue

        rem = REMEDIATION.search(line)
        if rem:
            fix = _clean(rem.group(1))
            for i in current_block_indices:
                out[i]["recommended_fix"] = fix
            continue

        # Inline error with embedded anomaly
        err = INLINE_ERROR.search(line)
        if err:
            label = err.group(1).lower()
            text  = err.group(2)
            typ = ("Throughput Drop" if "throughput" in label else
                   "Low RSRP"        if "low rsrp" in label else
                   "Low SINR"        if "low sinr" in label else
                   "UEs Spike/Drop")
            push(current[0], current[1], typ, text)
            continue

        # Regular anomaly bullets
        for rx, typ in ANOMALY_KEYS:
            k = rx.search(line)
            if k:
                push(current[0], current[1], typ, k.group(1))
                break

    # De-dupe (including fix in the signature so variants don’t collapse incorrectly)
    seen = set()
    uniq = []
    for r in out:
        sig = (r["cell_id"], r["band"], r["anomaly_type"], r["anomaly"], r.get("recommended_fix",""))
        if sig in seen:
            continue
        seen.add(sig)
        uniq.append(r)
    return uniq
