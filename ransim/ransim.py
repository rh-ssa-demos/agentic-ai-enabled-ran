"""
Author: Dinesh Lakshmanan
Date: 2025-06-04

Notes:
This script simulates a Radio Access Network (RAN) to generate realistic telemetry data,
including RAN Key Performance Indicators (KPIs) and Distributed Unit (DU) resource metrics.
It's designed to push this data to Apache Kafka topics for real-time processing and analysis.

Key features of this simulator:
- **Static Network Topology Loading**: The simulator loads a fixed RAN topology
  from an external 'cell_config.json' file at startup. This ensures that cell IDs,
  geographical locations (latitude/longitude), frequency bands, area types, cities,
  and adjacent cell relationships remain consistent across all simulation intervals and runs.
  The 'cell_config.json' file is generated separately by 'generate_static_cell_config.py'.
- **Dynamic KPI Generation**: For each simulation interval (e.g., every 1 minute),
  the simulator generates dynamic performance metrics (UEs usage, RSRP, RSRQ, SINR,
  Throughput, Latency) based on predefined usage patterns (industrial, commercial, rural, residential)
  and time of day/week.
- **DU Resource Metrics**: Alongside RAN KPIs, it simulates DU resource utilization
  (CPU, Memory, Disk Space, RTT, Temperature, Power Usage).
- **Anomaly Injection**: A configurable percentage of cells (currently 30%) are randomly
  selected in each interval to inject various types of anomalies (e.g., high PRB utilization,
  low RSRP, throughput drops, cell outages) to simulate real-world network issues.
- **Kafka Integration**: All generated metrics are formatted as CSV strings and produced
  to specified Kafka topics ('ran-combined-metrics' and 'du-resource-metrics').
- **Console Output**: Provides a sample of generated data directly in the console
  for quick verification.

This simulator is designed to run continuously, providing a steady stream of data
for monitoring, anomaly detection, and AI/ML model training in a simulated RAN environment.
"""


import random
import pandas as pd
import time
import csv
from datetime import datetime
from confluent_kafka import Producer
from rich.console import Console
from rich.table import Table
import json # <--- ADD THIS LINE: Import the JSON library

# Kafka Configuration (enhanced for batching and large messages)
conf = {
    'bootstrap.servers': 'my-cluster-kafka-bootstrap.amq-streams-kafka.svc:9092',
    #'bootstrap.servers': '192.168.154.101:30139',
    'client.id': 'ransim',
    'acks': 'all',
    'message.max.bytes': 10485760,
    'batch.size': 10485760,
    'linger.ms': 10
}
producer = Producer(conf)
topic_ran = "ran-combined-metrics"
topic_du = "du-resource-metrics"

# Frequency Bands (Keep this here; it's used to get 'Frequency' value)
BAND_FREQUENCY_MAP = {
    'Band 29': 700,
    'Band 26': 850,
    'Band 71': 600,
    'Band 66': '1700-2100'
}

# Usage Patterns (Keep this here; it's used for dynamic KPI calculation)
USAGE_PATTERNS = {
    'industrial': {'weekdays': {'day': (0.7, 0.9), 'night': (0.1, 0.3)},
                   'weekends': {'day': (0.4, 0.6), 'night': (0.1, 0.3)}},
    'commercial': {'weekdays': {'day': (0.3, 0.5), 'night': (0.6, 0.9)},
                   'weekends': {'day': (0.4, 0.6), 'night': (0.7, 0.9)}},
    'rural': {'weekdays': {'day': (0.1, 0.2), 'night': (0.1, 0.2)},
              'weekends': {'day': (0.1, 0.2), 'night': (0.1, 0.2)}},
    'residential': {'weekdays': {'day': (0.3, 0.2), 'night': (0.7, 0.9)},
                    'weekends': {'day': (0.4, 0.6), 'night': (0.5, 0.8)}}
}

console = Console()

def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[INFO] Message delivered to {msg.topic()} [{msg.partition()}]")

# <--- REMOVE OR COMMENT OUT THIS FUNCTION: We will load cells from JSON instead
# def generate_cells(num_cells=2000):
#     cells = []
#     ids = list(range(num_cells))
#     for cell_id in ids:
#         adjacent = random.sample([x for x in ids if x != cell_id], k=random.randint(1, 3))
#         cells.append({
#             'cell_id': int(cell_id),
#             'max_capacity': random.randint(50, 100),
#             'lat': round(random.uniform(37.7749, 37.8049), 6),
#             'lon': round(random.uniform(-122.4194, -122.3894), 6),
#             'bands': random.sample(list(BAND_FREQUENCY_MAP.keys()), k=random.randint(1, 3)),
#             'area_type': random.choice(list(USAGE_PATTERNS.keys())),
#             'adjacent_cells': adjacent
#         })
#     return cells
# <--- END REMOVAL/COMMENT OUT

def inject_anomaly(kpi, usage, current_band_normal_throughput=None, current_band_normal_usage=None):
    anomaly_type = random.choice([
        'high_prb_utilization', 'very_low_rsrp', 'sinr_degradation',
        'throughput_drop', 'ues_spike_drop', 'cell_outage'
    ])
    kpi['_anomaly_injected_type'] = anomaly_type

    if anomaly_type == 'high_prb_utilization':
        usage = int(kpi['Max Capacity'] * random.uniform(0.95, 1.0))
    elif anomaly_type == 'very_low_rsrp':
        kpi['RSRP'] = round(random.uniform(-140, -111), 2)
    elif anomaly_type == 'sinr_degradation':
        kpi['SINR'] = round(random.uniform(-10, -1), 2)
    elif anomaly_type == 'throughput_drop':
        baseline_throughput = current_band_normal_throughput if current_band_normal_throughput is not None else random.uniform(50, 100)
        kpi['Throughput (Mbps)'] = round(baseline_throughput * random.uniform(0.1, 0.4), 2)
        if kpi['Throughput (Mbps)'] < 0.1: kpi['Throughput (Mbps)'] = 0.1
    elif anomaly_type == 'ues_spike_drop':
        baseline_usage = current_band_normal_usage if current_band_normal_usage is not None else int(kpi['Max Capacity'] * random.uniform(0.3, 0.7))
        if random.random() < 0.5:
            usage = int(baseline_usage * random.uniform(0.1, 0.4))
        else:
            usage = int(kpi['Max Capacity'] * random.uniform(0.9, 1.0))
    elif anomaly_type == 'cell_outage':
        usage = 0
        kpi.update({'RSRP': 0, 'RSRQ': 0, 'SINR': 0, 'Throughput (Mbps)': 0, 'Latency (ms)': 0})
    return kpi, usage

def print_sample_output(df, title, color):
    table = Table(title=title, style=color)
    for col in df.columns:
        table.add_column(col, overflow='fold')
    for _, row in df.head(3).iterrows():
        table.add_row(*[str(row[col]) for col in df.columns])
    console.print(table)

def simulate(cells, interval=60):
    num_total_cells = len(cells)
    num_anomaly_cells = int(num_total_cells * 0.30)
    if num_anomaly_cells == 0 and num_total_cells > 0:
        num_anomaly_cells = 1

    while True:
        current_time = datetime.now()
        current_day = current_time.strftime('%A')
        ran_rows = []
        du_rows = []

        anomaly_cell_ids_this_interval = set()
        cell_band_anomaly_map = {}

        if num_total_cells > 0:
            # We need to ensure that the selection of anomaly cells is based on the *loaded* cells
            # and that they have bands, which they will from the config.
            anomaly_cells_selection = random.sample(cells, min(num_anomaly_cells, num_total_cells))

            for cell in anomaly_cells_selection:
                if cell['bands']:
                    cell_band_anomaly_map[cell['cell_id']] = random.choice(cell['bands'])
                    anomaly_cell_ids_this_interval.add(cell['cell_id'])

        for cell in cells: # This loop already iterates through your 'cells' list
            is_weekend = current_day in ['Saturday', 'Sunday']
            time_period = 'day' if 6 <= current_time.hour < 18 else 'night'
            usage_range = USAGE_PATTERNS[cell['area_type']][
                'weekends' if is_weekend else 'weekdays'][time_period]

            for band in cell['bands']: # 'bands' comes from the loaded cell config
                usage = int(random.uniform(*usage_range) * cell['max_capacity']) # 'max_capacity' comes from loaded cell config
                kpi = {
                    'RSRP': round(random.uniform(-120, -80), 2),
                    'RSRQ': round(random.uniform(-20, -3), 2),
                    'SINR': round(random.uniform(0, 30), 2),
                    'Throughput (Mbps)': round(random.uniform(10, 150), 2),
                    'Latency (ms)': round(random.uniform(10, 100), 2),
                    'Max Capacity': cell['max_capacity']
                }

                if cell['cell_id'] in anomaly_cell_ids_this_interval and band == cell_band_anomaly_map.get(cell['cell_id']):
                    kpi, usage = inject_anomaly(kpi, usage)
                    if '_anomaly_injected_type' in kpi:
                        del kpi['_anomaly_injected_type']

                ran_rows.append({
                    'Cell ID': cell['cell_id'], # from loaded config
                    'Datetime': current_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'Band': band, # from loaded config
                    'Frequency': BAND_FREQUENCY_MAP.get(band, 'Unknown'), # using BAND_FREQUENCY_MAP
                    'UEs Usage': usage,
                    'Area Type': cell['area_type'], # from loaded config
                    'Lat': cell['lat'], # <--- ADD THIS LINE: from loaded config
                    'Lon': cell['lon'], # <--- ADD THIS LINE: from loaded config
                    'City': cell['city'], # <--- ADD THIS LINE: from loaded config
                    'Adjacent Cells': ",".join(map(str, cell['adjacent_cells'])), # from loaded config
                    **kpi
                })

                du_rows.append({
                    'Cell ID': cell['cell_id'],
                    'Datetime': current_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'CPU (%)': round(random.uniform(10, 90), 2),
                    'Memory (MB)': random.randint(1000, 8000),
                    'Disk Space (GB)': round(random.uniform(10, 100), 2),
                    'RTT (ms)': round(random.uniform(1, 10), 2),
                    'Temperature (C)': round(random.uniform(30, 90), 2),
                    'Power Usage (W)': round(random.uniform(100, 500), 2)
                })

        df_ran = pd.DataFrame(ran_rows)
        df_du = pd.DataFrame(du_rows)

        producer.produce(topic_ran, key="ran", value=df_ran.to_csv(index=False, header=False), callback=delivery_report)
        producer.produce(topic_du, key="du", value=df_du.to_csv(index=False, header=False), callback=delivery_report)
        producer.flush()

        print_sample_output(df_ran, "Sample RAN + KPI Metrics", "cyan")
        print_sample_output(df_du, "Sample DU Resource Metrics", "magenta")

        planned_anomaly_cells_count = len(cell_band_anomaly_map)
        planned_anomalous_bands_count = sum(1 for band in cell_band_anomaly_map.values())

        print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] "
              f"Sent {len(df_ran)} RAN + KPI and {len(df_du)} DU entries.")
        print(f"*** VERIFICATION (Internal Planning) ***")
        print(f"    Expected Anomalous Cells: {num_anomaly_cells}")
        print(f"    Planned Unique Anomalous Cells: {planned_anomaly_cells_count}")
        print(f"    Planned Total Anomaly Records (Band-level): {planned_anomalous_bands_count}")
        print(f"    Planned Anomaly Cell IDs This Interval: {sorted(list(anomaly_cell_ids_this_interval))}")
        print(f"********************\n")

        print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] Sent {len(df_ran)} RAN + KPI and {len(df_du)} DU entries")
        time.sleep(interval)

if __name__ == "__main__":
    # --- START OF MODIFICATION ---
    # 1. Import the json library (already added at the top)
    # 2. Define the path to your cell configuration file
    cell_config_file = 'cell_config.json'

    # 3. Load the cell configuration from the JSON file
    try:
        with open(cell_config_file, 'r') as f:
            config_data = json.load(f)
            cells = config_data['cells'] # Extract the list of cell objects
        print(f"Successfully loaded {len(cells)} cells from {cell_config_file}.")
    except FileNotFoundError:
        print(f"Error: {cell_config_file} not found. Please ensure it's in the same directory and generated.")
        print("Run 'python generate_static_cell_config.py' first.")
        exit() # Exit the script if the config file isn't found

    # 4. Remove the call to generate_cells() as it's no longer needed
    # cells = generate_cells() # <--- REMOVE OR COMMENT OUT THIS LINE

    # --- END OF MODIFICATION ---

    # 5. The rest of the logic remains undisturbed!
    simulate(cells)