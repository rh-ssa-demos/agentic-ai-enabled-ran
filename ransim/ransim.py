"""
Author: Dinesh Lakshmanan
Email: dineshlakshmanan@redhat.com
Date: June 27, 2025

Notes:
This script simulates a Radio Access Network (RAN) to generate realistic telemetry data,
including RAN Key Performance Indicators (KPIs).
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
- **Anomaly Injection**: A configurable percentage of cells (currently 5%) are randomly
  selected in each interval to inject various types of anomalies (e.g., high PRB utilization,
  low RSRP, throughput drops, cell outages) to simulate real-world network issues.
- **Kafka Integration**: All generated metrics are formatted as **individual CSV strings** and produced
  as **separate messages** to a specified Kafka topic ('ran-combined-metrics').
- **Console Output**: Provides a sample of generated data directly in the console
  for quick verification.

This simulator is designed to run continuously, providing a steady stream of data
for monitoring, anomaly detection, and AI/ML model training in a simulated RAN environment.
"""

import os
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

KAFKA_HOST = os.getenv('KAFKA_HOST')

# Kafka Configuration (enhanced for batching and large messages)
conf = {
    #please change the bootstrap server according to your kafka configuration
    'bootstrap.servers': KAFKA_HOST,
    #'bootstrap.servers': '192.168.154.101:30139',
    'client.id': 'ransim',
    'acks': 'all',
    'message.max.bytes': 10485760,
    'batch.size': 10485760,
    'linger.ms': 10
}
producer = Producer(conf)
topic_ran = "ran-combined-metrics"
# topic_du = "du-resource-metrics" # <--- COMMENTED OUT: DU topic definition

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
        # Adjusted print to reflect only RAN topic if DU is commented out
        # if msg.topic() == topic_ran:
        print(f"[INFO] Message delivered to {msg.topic()} [{msg.partition()}]")
        # else:
        #     print(f"[INFO] Message delivered to {msg.topic()} [{msg.partition()}]")


# generate_cells function was already commented out or removed as per previous discussions.

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

def simulate(cells, interval=300):
    num_total_cells = len(cells)
    num_anomaly_cells = int(num_total_cells * 0.05)
    if num_anomaly_cells == 0 and num_total_cells > 0:
        num_anomaly_cells = 1

    while True:
        current_time = datetime.now()
        current_day = current_time.strftime('%A')
        
        ran_display_rows = []
        # du_display_rows = [] # <--- COMMENTED OUT: DU display rows

        anomaly_cell_ids_this_interval = set()
        cell_band_anomaly_map = {}

        if num_total_cells > 0:
            anomaly_cells_selection = random.sample(cells, min(num_anomaly_cells, num_total_cells))

            for cell in anomaly_cells_selection:
                if cell['bands']:
                    cell_band_anomaly_map[cell['cell_id']] = random.choice(cell['bands'])
                    anomaly_cell_ids_this_interval.add(cell['cell_id'])

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
                    normal_throughput = random.uniform(10, 150)
                    normal_usage = int(random.uniform(*usage_range) * cell['max_capacity'])
                    kpi, usage = inject_anomaly(kpi, usage, normal_throughput, normal_usage)
                    if '_anomaly_injected_type' in kpi:
                        del kpi['_anomaly_injected_type']  

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
                
                csv_buffer = StringIO()
                writer = csv.writer(csv_buffer, quoting=csv.QUOTE_ALL)
                 
                ran_values_ordered = [
                    ran_record_dict['Cell ID'], ran_record_dict['Datetime'], ran_record_dict['Band'],
                    ran_record_dict['Frequency'], ran_record_dict['UEs Usage'], ran_record_dict['Area Type'],
                    ran_record_dict['Lat'], ran_record_dict['Lon'], ran_record_dict['City'],
                    ran_record_dict['Adjacent Cells'], ran_record_dict['RSRP'], ran_record_dict['RSRQ'],
                    ran_record_dict['SINR'], ran_record_dict['Throughput (Mbps)'], ran_record_dict['Latency (ms)'],
                    ran_record_dict['Max Capacity']
                ]
                writer.writerow(ran_values_ordered)
                ran_csv_string = csv_buffer.getvalue().strip()

                producer.produce(
                    topic_ran, 
                    value=ran_csv_string.encode('utf-8'),
                    callback=delivery_report
                )
                ran_display_rows.append(ran_record_dict)

                # # Create DU record # <--- COMMENTED OUT: DU record creation block
                # du_record_dict = {
                #     'Cell ID': cell['cell_id'],
                #     'Datetime': current_time.strftime('%Y-%m-%d %H:%M:%S'),
                #     'CPU (%)': round(random.uniform(10, 90), 2),
                #     'Memory (MB)': random.randint(1000, 8000),
                #     'Disk Space (GB)': round(random.uniform(10, 100), 2),
                #     'RTT (ms)': round(random.uniform(1, 10), 2),
                #     'Temperature (C)': round(random.uniform(30, 90), 2),
                #     'Power Usage (W)': round(random.uniform(100, 500), 2)
                # }

                # # --- Convert DU record dictionary to CSV string ---
                # du_csv_buffer = StringIO()
                # du_writer = csv.writer(du_csv_buffer, quoting=csv.QUOTE_ALL)
                # du_values_ordered = [
                #     du_record_dict['Cell ID'], du_record_dict['Datetime'], du_record_dict['CPU (%)'],
                #     du_record_dict['Memory (MB)'], du_record_dict['Disk Space (GB)'], du_record_dict['RTT (ms)'],
                #     du_record_dict['Temperature (C)'], du_record_dict['Power Usage (W)']
                # ]
                # du_writer.writerow(du_values_ordered)
                # du_csv_string = du_csv_buffer.getvalue().strip()

                # # Produce DU record as a single Kafka message
                # producer.produce(
                #     topic_du,
                #     value=du_csv_string.encode('utf-8'),
                #     callback=delivery_report
                # )
                # du_display_rows.append(du_record_dict)

        producer.flush() # Flush any remaining buffered messages after all records are sent for the interval 

        # Create DataFrames from the collected sample rows for console output
        df_ran_sample = pd.DataFrame(ran_display_rows)
        # df_du_sample = pd.DataFrame(du_display_rows) # <--- COMMENTED OUT: DU DataFrame

        print_sample_output(df_ran_sample, "Sample RAN + KPI Metrics", "cyan")
        # print_sample_output(df_du_sample, "Sample DU Resource Metrics", "magenta") # <--- COMMENTED OUT: DU print output

        planned_anomaly_cells_count = len(cell_band_anomaly_map)
        planned_anomalous_bands_count = sum(1 for band in cell_band_anomaly_map.values())

        # Adjusted print statement for sent entries
        print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] "
              f"Sent {len(ran_display_rows)} RAN + KPI entries as individual Kafka messages.") # <--- MODIFIED PRINT
        print(f"*** VERIFICATION (Internal Planning) ***")
        print(f"    Expected Anomalous Cells: {num_anomaly_cells}")
        print(f"    Planned Unique Anomalous Cells: {planned_anomaly_cells_count}")
        print(f"    Planned Total Anomaly Records (Band-level): {planned_anomalous_bands_count}")
        print(f"    Planned Anomaly Cell IDs This Interval: {sorted(list(anomaly_cell_ids_this_interval))}")
        print(f"********************\n")

        # Adjusted print statement for sent entries
        print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] Sent {len(ran_display_rows)} RAN + KPI entries") # <--- MODIFIED PRINT
        time.sleep(interval)

if __name__ == "__main__":
    cell_config_file = 'cell_config.json'

    try:
        with open(cell_config_file, 'r') as f:
            config_data = json.load(f)
            cells = config_data['cells'] 
        print(f"Successfully loaded {len(cells)} cells from {cell_config_file}.")
    except FileNotFoundError:
        print(f"Error: {cell_config_file} not found. Please ensure it's in the same directory and generated.")
        print("Run 'python generate_static_cell_config.py' first.")
        exit()

    simulate(cells)
