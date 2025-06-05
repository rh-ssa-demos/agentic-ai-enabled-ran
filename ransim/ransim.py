"""
Author: Dinesh Lakshmanan
Email: dineshlakshmanan@redhat.com
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
- **Kafka Integration**: All generated metrics are formatted as **individual CSV strings** and produced
  as **separate messages** to specified Kafka topics ('ran-combined-metrics' and 'du-resource-metrics').
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
from io import StringIO # REQUIRED: Import StringIO for CSV serialization

# Kafka Configuration (enhanced for batching and large messages)
conf = {
    #'bootstrap.servers': 'my-cluster-kafka-bootstrap.amq-streams-kafka.svc:9092',
    'bootstrap.servers': '192.168.154.101:30139',
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
    # This remains at 30% as per your specified latest code.
    num_anomaly_cells = int(num_total_cells * 0.30)
    if num_anomaly_cells == 0 and num_total_cells > 0:
        num_anomaly_cells = 1

    while True:
        current_time = datetime.now()
        current_day = current_time.strftime('%A')
        
        # ran_rows and du_rows are now used to collect *all* records for display
        # and individual Kafka production, not for single large Kafka message.
        ran_display_rows = []
        du_display_rows = []

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

        # --- MODIFICATION START: Loop through cells and bands to produce individual Kafka messages ---
        for cell in cells: 
            is_weekend = current_day in ['Saturday', 'Sunday']
            time_period = 'day' if 6 <= current_time.hour < 18 else 'night'
            usage_range = USAGE_PATTERNS[cell['area_type']][
                'weekends' if is_weekend else 'weekdays'][time_period]

            for band in cell['bands']: 
                usage = int(random.uniform(*usage_range) * cell['max_capacity']) 
                kpi = {
                    'RSRP': round(random.uniform(-120, -80), 2),
                    'RSRQ': round(random.uniform(-20, -3), 2),
                    'SINR': round(random.uniform(0, 30), 2),
                    'Throughput (Mbps)': round(random.uniform(10, 150), 2),
                    'Latency (ms)': round(random.uniform(10, 100), 2),
                    'Max Capacity': cell['max_capacity']
                }

                if cell['cell_id'] in anomaly_cell_ids_this_interval and band == cell_band_anomaly_map.get(cell['cell_id']):
                    # Note: current_band_normal_throughput/usage are placeholders for a more complex simulation
                    normal_throughput = random.uniform(10, 150)
                    normal_usage = int(random.uniform(*usage_range) * cell['max_capacity'])
                    kpi, usage = inject_anomaly(kpi, usage, normal_throughput, normal_usage)
                    if '_anomaly_injected_type' in kpi:
                        del kpi['_anomaly_injected_type'] 

                # Create the full record dictionary for this single cell-band combination
                ran_record_dict = {
                    'Cell ID': cell['cell_id'], 
                    'Datetime': current_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'Band': band, 
                    'Frequency': BAND_FREQUENCY_MAP.get(band, 'Unknown'), 
                    'UEs Usage': usage,
                    'Area Type': cell['area_type'], 
                    'Lat': cell['lat'], 
                    'Lon': cell['lon'], 
                    'City': cell['city'], 
                    'Adjacent Cells': ",".join(map(str, cell['adjacent_cells'])), 
                    **kpi
                }
                
                # --- Convert record dictionary to CSV string ---
                # This ensures each Kafka message value is a single CSV line.
                csv_buffer = StringIO()
                # Use a csv.writer to handle quoting for fields with commas (like 'Adjacent Cells')
                writer = csv.writer(csv_buffer, quoting=csv.QUOTE_ALL)
                
                # Ensure the order of values matches your consumer's expected column_names
                # You'll need to define a consistent order of values to write
                # Example:
                ran_values_ordered = [
                    ran_record_dict['Cell ID'], ran_record_dict['Datetime'], ran_record_dict['Band'],
                    ran_record_dict['Frequency'], ran_record_dict['UEs Usage'], ran_record_dict['Area Type'],
                    ran_record_dict['Lat'], ran_record_dict['Lon'], ran_record_dict['City'],
                    ran_record_dict['Adjacent Cells'], ran_record_dict['RSRP'], ran_record_dict['RSRQ'],
                    ran_record_dict['SINR'], ran_record_dict['Throughput (Mbps)'], ran_record_dict['Latency (ms)'],
                    ran_record_dict['Max Capacity']
                ]
                writer.writerow(ran_values_ordered)
                ran_csv_string = csv_buffer.getvalue().strip() # .strip() to remove trailing newline

                # Produce the single CSV string as a Kafka message
                # Use Cell ID as the key to distribute messages across partitions
                producer.produce(
                    topic_ran,
                    #key=str(ran_record_dict['Cell ID']).encode('utf-8'), # Key must be bytes
                    value=ran_csv_string.encode('utf-8'), # Value is a single CSV line (bytes)
                    callback=delivery_report
                )
                ran_display_rows.append(ran_record_dict) # Add dict to sample list for console output

                # Create DU record
                du_record_dict = {
                    'Cell ID': cell['cell_id'],
                    'Datetime': current_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'CPU (%)': round(random.uniform(10, 90), 2),
                    'Memory (MB)': random.randint(1000, 8000),
                    'Disk Space (GB)': round(random.uniform(10, 100), 2),
                    'RTT (ms)': round(random.uniform(1, 10), 2),
                    'Temperature (C)': round(random.uniform(30, 90), 2),
                    'Power Usage (W)': round(random.uniform(100, 500), 2)
                }

                # --- Convert DU record dictionary to CSV string ---
                du_csv_buffer = StringIO()
                du_writer = csv.writer(du_csv_buffer, quoting=csv.QUOTE_ALL)
                du_values_ordered = [
                    du_record_dict['Cell ID'], du_record_dict['Datetime'], du_record_dict['CPU (%)'],
                    du_record_dict['Memory (MB)'], du_record_dict['Disk Space (GB)'], du_record_dict['RTT (ms)'],
                    du_record_dict['Temperature (C)'], du_record_dict['Power Usage (W)']
                ]
                du_writer.writerow(du_values_ordered)
                du_csv_string = du_csv_buffer.getvalue().strip()

                # Produce DU record as a single Kafka message
                producer.produce(
                    topic_du,
                    #key=str(du_record_dict['Cell ID']).encode('utf-8'), # Key must be bytes
                    value=du_csv_string.encode('utf-8'), # Value is a single CSV line (bytes)
                    callback=delivery_report
                )
                du_display_rows.append(du_record_dict) # Add dict to sample list for console output

        producer.flush() # Flush any remaining buffered messages after all records are sent for the interval

        # --- MODIFICATION END ---

        # Create DataFrames from the collected sample rows for console output
        # These are just for printing; Kafka messages were sent individually above.
        df_ran_sample = pd.DataFrame(ran_display_rows)
        df_du_sample = pd.DataFrame(du_display_rows)

        print_sample_output(df_ran_sample, "Sample RAN + KPI Metrics", "cyan")
        print_sample_output(df_du_sample, "Sample DU Resource Metrics", "magenta")

        planned_anomaly_cells_count = len(cell_band_anomaly_map)
        planned_anomalous_bands_count = sum(1 for band in cell_band_anomaly_map.values())

        print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] "
              f"Sent {len(ran_display_rows)} RAN + KPI and {len(du_display_rows)} DU entries as individual Kafka messages.")
        print(f"*** VERIFICATION (Internal Planning) ***")
        print(f"    Expected Anomalous Cells: {num_anomaly_cells}")
        print(f"    Planned Unique Anomalous Cells: {planned_anomaly_cells_count}")
        print(f"    Planned Total Anomaly Records (Band-level): {planned_anomalous_bands_count}")
        print(f"    Planned Anomaly Cell IDs This Interval: {sorted(list(anomaly_cell_ids_this_interval))}")
        print(f"********************\n")

        print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] Sent {len(ran_display_rows)} RAN + KPI and {len(du_display_rows)} DU entries")
        time.sleep(interval) # Wait for the next simulation interval

if __name__ == "__main__":
    # --- START OF MODIFICATION ---
    # 1. Import the json library (already added at the top)
    # 2. Define the path to your cell configuration file
    cell_config_file = 'cell_config.json'

    # 3. Load the cell configuration from the JSON file
    try:
        with open(cell_config_file, 'r') as f:
            config_data = json.load(f)
            cells = config_data['cells'] 
        print(f"Successfully loaded {len(cells)} cells from {cell_config_file}.")
    except FileNotFoundError:
        print(f"Error: {cell_config_file} not found. Please ensure it's in the same directory and generated.")
        print("Run 'python generate_static_cell_config.py' first.")
        exit() 

    # 4. Remove the call to generate_cells() as it's no longer needed
    # cells = generate_cells() # <--- REMOVE OR COMMENT OUT THIS LINE

    # --- END OF MODIFICATION ---

    # 5. The rest of the logic remains undisturbed!
    simulate(cells)