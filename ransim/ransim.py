import random
import pandas as pd
import time
import csv
from datetime import datetime
from confluent_kafka import Producer
from rich.console import Console
from rich.table import Table

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

# Frequency Bands
BAND_FREQUENCY_MAP = {
    'Band 29': 700,
    'Band 26': 850,
    'Band 71': 600,
    'Band 66': '1700-2100'
}

# Usage Patterns
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

def generate_cells(num_cells=2000):
    cells = []
    ids = list(range(num_cells))
    for cell_id in ids:
        adjacent = random.sample([x for x in ids if x != cell_id], k=random.randint(1, 3))
        cells.append({
            'cell_id': int(cell_id),
            'max_capacity': random.randint(50, 100),
            'lat': round(random.uniform(37.7749, 37.8049), 6),
            'lon': round(random.uniform(-122.4194, -122.3894), 6),
            'bands': random.sample(list(BAND_FREQUENCY_MAP.keys()), k=random.randint(1, 3)),
            'area_type': random.choice(list(USAGE_PATTERNS.keys())),
            'adjacent_cells': adjacent
        })
    return cells

def inject_anomaly(kpi, usage):
    anomaly_type = random.choice([
        'high_prb_utilization', 'very_low_rsrp', 'sinr_degradation',
        'throughput_drop', 'ues_spike_drop', 'cell_outage'
    ])
    # Store the anomaly type for verification
    # Note: We're adding it to the kpi dict, which will then be used to construct the row.
    kpi['_anomaly_injected_type'] = anomaly_type # Renamed to avoid confusion with df column name

    if anomaly_type == 'high_prb_utilization':
        usage = int(kpi['Max Capacity'] * random.uniform(0.95, 1.0))
    elif anomaly_type == 'very_low_rsrp':
        kpi['RSRP'] = round(random.uniform(-140, -111), 2)
    elif anomaly_type == 'sinr_degradation':
        kpi['SINR'] = round(random.uniform(-10, -1), 2)
    elif anomaly_type == 'throughput_drop':
        kpi['Throughput (Mbps)'] = round(random.uniform(0.1, 5), 2)
    elif anomaly_type == 'ues_spike_drop':
        usage = random.choice([0, kpi['Max Capacity']])
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
    num_anomaly_cells = int(num_total_cells * 0.05) # Calculate 5% of total cells
    if num_anomaly_cells == 0 and num_total_cells > 0: # Ensure at least one if cells exist
        num_anomaly_cells = 1

    while True:
        current_time = datetime.now()
        current_day = current_time.strftime('%A')
        ran_rows = []
        du_rows = []

        # --- Anomaly Injection Setup for this Interval ---
        anomaly_cell_ids_this_interval = set() # Renamed for clarity in this scope
        cell_band_anomaly_map = {} # Maps cell_id to the specific band that will have an anomaly

        if num_total_cells > 0:
            anomaly_cells_selection = random.sample(cells, min(num_anomaly_cells, num_total_cells))

            for cell in anomaly_cells_selection:
                if cell['bands']:
                    cell_band_anomaly_map[cell['cell_id']] = random.choice(cell['bands'])
                    anomaly_cell_ids_this_interval.add(cell['cell_id']) # Store the IDs of affected cells
        # --- End Anomaly Injection Setup ---

        '''
        # --- Anomaly Injection Logic ---
        # Step 1: Randomly select 5% of the cells to inject an anomaly into this interval
        anomaly_cell_ids = set()
        if num_total_cells > 0:
            anomaly_cells_selection = random.sample(cells, min(num_anomaly_cells, num_total_cells))
            anomaly_cell_ids = {cell['cell_id'] for cell in anomaly_cells_selection}

        # Step 2: For each selected anomaly cell, choose one random band within it
        cell_band_anomaly_map = {} # Maps cell_id to the specific band that will have an anomaly
        for cell in anomaly_cells_selection:
            if cell['bands']: # Ensure the cell actually has bands
                cell_band_anomaly_map[cell['cell_id']] = random.choice(cell['bands'])
            else:
                # If a chosen cell has no bands, it won't receive an anomaly in this specific run,
                # or you could handle this by picking another cell if needed, but for simplicity,
                # we'll just skip it for this interval.
                pass
        # --- End Anomaly Injection Logic ---
        '''

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
                #kpi['_anomaly_injected'] = None # Initialize anomaly flag to None
                # Default anomaly status for this band of this cell
                #anomaly_type_for_this_row = None

                # Check if this cell and band should receive an anomaly
                if cell['cell_id'] in anomaly_cell_ids_this_interval and band == cell_band_anomaly_map.get(cell['cell_id']):
                    kpi, usage = inject_anomaly(kpi, usage)
                    # Importantly, remove the internal flag *after* anomaly injection,
                    # so it doesn't appear in your final DataFrame.
                    if '_anomaly_injected_type' in kpi:
                        del kpi['_anomaly_injected_type']

                '''
                # Apply anomaly if this cell is one of the chosen anomaly cells for this interval
                # AND this specific band is the chosen anomaly band for this cell
                if cell['cell_id'] in anomaly_cell_ids and band == cell_band_anomaly_map.get(cell['cell_id']):
                    kpi, usage = inject_anomaly(kpi, usage)
                    # print(f"DEBUG: Anomaly injected in Cell ID: {cell['cell_id']}, Band: {band}") # Optional debug print
                '''

                ran_rows.append({
                    'Cell ID': cell['cell_id'],
                    'Datetime': current_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'Band': band,
                    'Frequency': BAND_FREQUENCY_MAP.get(band, 'Unknown'),
                    'UEs Usage': usage,
                    'Area Type': cell['area_type'],
                    'Adjacent Cells': ",".join(map(str, cell['adjacent_cells'])),
                    #'Anomaly Type': anomaly_type_for_this_row, # Use the captured variable
                    #'Adjacent Cells': "|".join(map(str, cell['adjacent_cells'])),
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

        # If required set header=True to have a header row for the csv
        producer.produce(topic_ran, key="ran", value=df_ran.to_csv(index=False, header=False), callback=delivery_report)
        #producer.produce(topic_ran, key="ran", value=df_ran.to_csv(index=False, header=False, quoting=csv.QUOTE_ALL), callback=delivery_report)
        producer.produce(topic_du, key="du", value=df_du.to_csv(index=False, header=False), callback=delivery_report)
        producer.flush()

        print_sample_output(df_ran, "Sample RAN + KPI Metrics", "cyan")
        print_sample_output(df_du, "Sample DU Resource Metrics", "magenta")

        # --- Verification Logic (for your console, not the Kafka data) ---
        # To verify anomalies when the column isn't in the DF, you'd need to store
        # which cells/bands were affected separately during the generation loop.
        # Here, we'll just report based on the 'cell_band_anomaly_map' directly.

        # Count unique Cell IDs that were *planned* to have an anomaly this interval
        planned_anomaly_cells_count = len(cell_band_anomaly_map)

        # Count total anomalous records (bands) *planned* for this interval
        planned_anomalous_bands_count = sum(1 for band in cell_band_anomaly_map.values())


        print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] "
              f"Sent {len(df_ran)} RAN + KPI and {len(df_du)} DU entries.")
        print(f"*** VERIFICATION (Internal Planning) ***")
        print(f"    Expected Anomalous Cells: {num_anomaly_cells}")
        print(f"    Planned Unique Anomalous Cells: {planned_anomaly_cells_count}")
        print(f"    Planned Total Anomaly Records (Band-level): {planned_anomalous_bands_count}")
        print(f"    Planned Anomaly Cell IDs This Interval: {sorted(list(anomaly_cell_ids_this_interval))}")
        # print(f"    Anomaly Cell-Band Map: {cell_band_anomaly_map}") # Uncomment for detailed map
        print(f"********************\n")
        # --- End Verification Logic ---


        print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] Sent {len(df_ran)} RAN + KPI and {len(df_du)} DU entries")
        time.sleep(interval)

if __name__ == "__main__":
    cells = generate_cells()
    simulate(cells)
