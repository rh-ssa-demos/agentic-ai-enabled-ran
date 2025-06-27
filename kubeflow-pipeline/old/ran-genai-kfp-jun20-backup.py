from kfp import dsl
from kfp.dsl import component, Input, Output, Dataset
from kfp import components
from kfp.compiler import Compiler
#from kfp.v2.dsl import component


# Define components here (kafka_to_s3, train_lstm_kpi_model, train_traffic_predictor, predict_traffic_levels, predict_kpi_anomalies, genai_traffic_forecast_explainer)

# ─────────────────────────────────────────────────────────────── #
#  Stream Kafka-ran-metrics-to-S3 Component

@component(
    base_image="python:3.9",
    packages_to_install=["boto3", "kafka-python", "pandas", "tabulate"]
)
def stream_ran_metrics_to_s3_component(
    bootstrap_servers: str,
    s3_bucket: str,
    s3_key_prefix: str,
    s3_endpoint: str,
    aws_access_key: str,
    aws_secret_key: str,
    ran_metrics: Output[Dataset],
    #max_records: int = 500
    max_records: int = 30
):
    import os
    import csv
    import boto3
    import pandas as pd
    from kafka import KafkaConsumer
    from kafka import TopicPartition
    from datetime import datetime
    from io import StringIO
    from tabulate import tabulate
    import html
    import time

    os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_key

    s3_client = boto3.client('s3', endpoint_url=s3_endpoint)
    topic_name = "ran-combined-metrics"

    # --- Configuration for specific partition consumption ---
    target_partition = 0 # The partition where your producer is sending all data
    consumer_group_id = 'kfp-ran-metrics-s3-consumer-group-p0-only' # Use a new, distinct group_id for this strategy

    column_names = [
        "Cell ID", "Datetime", "Band", "Frequency", "UEs Usage", "Area Type", "Lat", "Lon", "City", "Adjacent Cells", 
        "RSRP", "RSRQ", "SINR", "Throughput (Mbps)", "Latency (ms)", "Max Capacity"
    ]

    #consumer_group_id = 'kfp-ran-metrics-s3-consumer-group-001'
    print(f"Connecting to Kafka topic '{topic_name}' with group ID '{consumer_group_id}'...")
    consumer = None # Initialize consumer to None

    try:
        #print(f"Connecting to Kafka topic '{topic_name}'...")
        consumer = KafkaConsumer(
            #topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id=consumer_group_id, # THIS IS WHAT group.py IS CHECKING FOR
            auto_offset_reset='earliest',
            #auto_offset_reset='latest',
            #enable_auto_commit=True,
            enable_auto_commit=False,
            value_deserializer=lambda m: m.decode('utf-8', errors='ignore') if m else None,
            # Fetch just one Kafka message (as it contains all your 2000+ records)
            # We need to limit the number of *Kafka messages* to 1 if one Kafka message = 2000 lines
            #max_poll_records=1, # <<< Set this to 1 because each Kafka message is one large CSV block
            max_poll_records=30, # <<< Consumer will fetch 30 individual kafka messages 
            client_id=f"kfp-comp-{os.getpid()}-{int(time.time())}-p{target_partition}"
        )

        # --- CRITICAL CHANGE: Use subscribe() ---
        consumer.subscribe([topic_name])
        print(f"Subscribed to topic '{topic_name}'. Waiting for partition assignment...")

        assigned_partitions = []
        timeout_start = time.time()
        timeout_duration = 60 # seconds to wait for assignment
        while not assigned_partitions and (time.time() - timeout_start < timeout_duration):
            consumer.poll(timeout_ms=500) 
            assigned_partitions = consumer.assignment()
            if not assigned_partitions:
                print(f"Waiting for partition assignment... ({int(time.time() - timeout_start)}s elapsed)")
                time.sleep(1) 
        
        if not assigned_partitions:
            raise RuntimeError(f"Consumer failed to get partition assignment within {timeout_duration} seconds. Exiting.")

        print(f"Successfully assigned partitions: {assigned_partitions}")

        tp = TopicPartition(topic_name, target_partition) 
        assigned_tp_obj = None
        for p in assigned_partitions:
            if p.topic == topic_name and p.partition == target_partition:
                assigned_tp_obj = p
                break

        if assigned_tp_obj: 
            committed_offset = consumer.committed(assigned_tp_obj)
            if committed_offset is not None:
                consumer.seek(assigned_tp_obj, committed_offset)
                print(f"Found committed offset {committed_offset} for {assigned_tp_obj}. Seeking to it.")
            else:
                consumer.seek_to_beginning(assigned_tp_obj) 
                print(f"No committed offset found for {assigned_tp_obj}. Seeking to beginning.")
            
            current_position = consumer.position(assigned_tp_obj)
            print(f"Current position for {assigned_tp_obj}: {current_position}")
        else:
            print(f"WARNING: Target partition {target_partition} not assigned to this consumer. This run may not process data.")
            return 

        collected_lines = []
        total_parsed_records_in_run = 0

        print(f"\n Streaming Kafka Records (targeting up to {max_records} CSV records per run):\n")

        while total_parsed_records_in_run < max_records:
            messages_from_kafka_poll = consumer.poll(timeout_ms=5000) 

            if not messages_from_kafka_poll:
                print("No more Kafka messages in this poll after timeout. Breaking processing loop.")
                break 

            for current_tp_key, msgs_list in messages_from_kafka_poll.items():
                if current_tp_key.topic == topic_name and current_tp_key.partition == target_partition: 
                    for message in msgs_list:
                        if not message.value:
                            print("Skipping empty Kafka message...")
                            continue

                        individual_csv_lines = message.value.strip().splitlines()

                        for line in individual_csv_lines:
                            if not line:
                                continue

                            if total_parsed_records_in_run >= max_records:
                                print(f"Reached {max_records} logical CSV records. Stopping processing for this run.")
                                break 

                            collected_lines.append(line)
                            total_parsed_records_in_run += 1

                            try:
                                # Explicitly set delimiter and quotechar for robustness
                                csv_reader_obj = csv.reader([line], delimiter=',', quotechar='"')
                                values_for_print = next(csv.reader([line]))
                                if len(values_for_print) == len(column_names):
                                    if total_parsed_records_in_run == 1:
                                        print("\n" + ", ".join(column_names))
                                    print(", ".join(str(v) for v in values_for_print))
                                else:
                                    # THIS IS CRITICAL: Print the actual number of parsed values
                                    print(f"Record {total_parsed_records_in_run}: [Invalid format for print] Expected {len(column_names)} columns ({column_names}), got {len(values_for_print)}: {values_for_print} | Original Line: {line}")
                            except Exception as e:
                                print(f"Record {total_parsed_records_in_run}: [Error parsing for print] {e} | Content: {line}")
                        
                        # NO COMMIT HERE. We will commit at the end of the run.
                        
                        if total_parsed_records_in_run >= max_records:
                            break
                    
                if total_parsed_records_in_run >= max_records:
                    break
            
            if not messages_from_kafka_poll and total_parsed_records_in_run < max_records:
                print("No more messages found in topic after initial fetch. Exiting polling loop.")
                break

        print(f"Successfully processed {total_parsed_records_in_run} logical CSV records in this run.")

        if not collected_lines:
            print("No valid records collected. Exiting without upload.")
            return

        records = []
        for line in collected_lines:
            try:
                parsed = next(csv.reader([line]))
                if len(parsed) == len(column_names):
                    records.append(parsed)
                else:
                    print(f"Skipping malformed record due to column mismatch: Expected {len(column_names)}, got {len(parsed)} - Raw: '{line}' - Parsed: {parsed}")
            except Exception as e:
                print(f"CSV parsing failed: {e} | Content: '{line}'")

        df = pd.DataFrame(records, columns=column_names)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_object_key = f"{s3_key_prefix}/ran-combined-metrics/{topic_name}_{timestamp}.csv"

        if 'Adjacent Cells' in df.columns:
            df['Adjacent Cells'] = df['Adjacent Cells'].apply(
                lambda x: ','.join(
                    map(lambda c: str(c).replace('"', '""'), x)
                ) if isinstance(x, list) else str(x).replace('"', '""')
            )

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_ALL)

        csv_string = csv_buffer.getvalue()
        print(csv_string) 

        s3_client.put_object(Bucket=s3_bucket, Key=s3_object_key, Body=csv_buffer.getvalue())
        s3_uri = f"s3://{s3_bucket}/{s3_object_key}"
        print(f"\nData successfully uploaded to {s3_uri}")

        with open(ran_metrics.path, 'w') as f:
            df.to_csv(f, index=False, header=True)

    except Exception as outer_e:
        print(f"An unexpected error occurred during component execution: {outer_e}")
        raise
    finally:
        # --- FINAL COMMIT & CLOSE ---
        # Perform a final commit for the entire batch after all processing and S3 upload
        # This is the most reliable place to commit for short-lived components.
        if consumer and total_parsed_records_in_run > 0: # Only commit if consumer exists and processed data
            try:
                consumer.commit() # Commit the latest polled offset for assigned partitions
                print(f"FINAL COMMIT: Successfully committed offsets for the entire batch.")
            except Exception as e:
                print(f"FINAL COMMIT ERROR: Failed to commit offsets: {e}. Data might be reprocessed.")
                # You might want to log this but not necessarily re-raise if you prefer the component to succeed.
        
        if consumer: 
            consumer.close()
            print("Consumer closed.")


    '''
    print("\n Streaming Kafka Records:\n")
    for message in consumer:
        if not message.value:
            print("Skipping empty message...")
            continue
        lines = message.value.strip().splitlines()
        for line in lines:
            if line:
                collected_lines.append(line)
                total_records += 1

                #  Parse and print nicely
                try:
                    values = next(csv.reader([line]))
                    if len(values) == len(column_names):
                        record_dict = dict(zip(column_names, values))
                        if total_records == 1:
                            print("\n" + ", ".join(column_names))  # print header once
                        print(", ".join(str(record_dict[col]) for col in column_names))
                    else:
                        print(f"Record {total_records}: [Invalid format] {line}")
                except Exception as e:
                    print(f"Record {total_records}: [Error parsing] {e}")


            if total_records >= max_records:
                print(f"\n Reached max_records={max_records}. Breaking.\n")
                break
        if total_records >= max_records:
            break
    consumer.close()
    '''


    #return s3_uri
# ─────────────────────────────────────────────────────────────── #
#  Stream Kafka-du-metrics-to-S3 Component
@component(
    base_image="python:3.9",
    packages_to_install=["boto3", "kafka-python", "pandas", "tabulate"]
)
def stream_du_metrics_to_s3_component(
    bootstrap_servers: str,
    s3_bucket: str,
    s3_key_prefix: str,
    s3_endpoint: str,
    aws_access_key: str,
    aws_secret_key: str,
    du_metrics: Output[Dataset],
    max_records: int = 500
):
    import os
    import csv
    import boto3
    import pandas as pd
    from kafka import KafkaConsumer
    from datetime import datetime
    from io import StringIO
    from tabulate import tabulate

    os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_key

    s3_client = boto3.client('s3', endpoint_url=s3_endpoint)
    topic_name = "du-resource-metrics"

    column_names = [
        "Cell ID", "Datetime", "CPU (%)", "Memory (MB)", "Disk Space (GB)", 
        "RTT (ms)", "Temperature (C)", "Power Usage (W)"
    ]

    print(f"Connecting to Kafka topic '{topic_name}'...")
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: m.decode('utf-8', errors='ignore') if m else None
    )
    print(f"Connected to Kafka topic '{topic_name}'")

    collected_lines = []
    total_records = 0

    print("\n Streaming Kafka Records:\n")

    for message in consumer:
        if not message.value:
            print("Skipping empty message...")
            continue
        lines = message.value.strip().splitlines()
        for line in lines:
            if line:
                collected_lines.append(line)
                total_records += 1

                #  Parse and print nicely
                try:
                    values = next(csv.reader([line]))
                    if len(values) == len(column_names):
                        record_dict = dict(zip(column_names, values))
                        if total_records == 1:
                            print("\n" + ", ".join(column_names))  # print header once
                        print(", ".join(str(record_dict[col]) for col in column_names))
                    else:
                        print(f"Record {total_records}: [Invalid format] {line}")
                except Exception as e:
                    print(f"Record {total_records}: [Error parsing] {e}")

            if total_records >= max_records:
                print(f"\n Reached max_records={max_records}. Breaking.\n")
                break
        if total_records >= max_records:
            break
    consumer.close()

    if not collected_lines:
        print("No valid records collected. Exiting without upload.")
        return

    # Combine all lines into a DataFrame
    #print(f"\n combining all lines into a DataFrame")
    csv_data = "\n".join(collected_lines)
    df = pd.read_csv(StringIO(csv_data), header=None)

    # Upload to S3
    #print(f"\n Uploading to S3")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_object_key = f"{s3_key_prefix}/du-resource-metrics/{topic_name}_{timestamp}.csv"
    print(f"\n Data uploading to s3")

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    s3_client.put_object(Bucket=s3_bucket, Key=s3_object_key, Body=csv_buffer.getvalue())
    s3_uri = f"s3://{s3_bucket}/{s3_object_key}"
    print(f"\n Data successfully uploaded to {s3_uri}")

    # Output to kubeflow artifacts
    with open(du_metrics.path, 'w') as f:
        df.to_csv(f, index=False, header=True)

    #return s3_uri

# ─────────────────────────────────────────────────────────────── #
# Train RandomForest trafic Prediction Model  Component
#from kfp.v2.dsl import component, Output, Dataset
# Define the component
@component(
    base_image='python:3.9',
    packages_to_install=['pandas', 'scikit-learn', 'boto3', 'joblib', 'numpy']
)
def train_traffic_predictor(
    s3_bucket: str,
    s3_key_prefix: str,
    s3_endpoint: str,
    aws_access_key: str,
    aws_secret_key: str,
    output_traffic_regressor_model: Output[Dataset],
    output_traffic_classifier_model: Output[Dataset]
):
    import pandas as pd
    import boto3
    import joblib
    import io
    import os
    import logging
    from datetime import datetime
    import numpy as np
    import csv
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import classification_report, mean_absolute_error

    # Logging setup
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] - %(message)s')
    log = logging.getLogger()

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    log.info("Traffic predictor training started.")

    # Setup AWS
    os.environ.update({
        'AWS_ACCESS_KEY_ID': aws_access_key,
        'AWS_SECRET_ACCESS_KEY': aws_secret_key,
    })
    s3 = boto3.client('s3', endpoint_url=s3_endpoint)
    log.info("AWS credentials and environment variables configured.")

    def load_all_ran_files_from_s3(s3, s3_bucket, s3_key_prefix):
        """
        Load and concatenate all RAN CSV metric files from S3 that match the prefix.
        Assumes all files follow the same structure and headers.
        """
        # List and sort files
        files = sorted([
            o['Key'] for o in s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key_prefix).get('Contents', [])
            if "ran-combined-metrics" in o['Key']
        ])
        
        if not files:
            raise FileNotFoundError("No RAN metrics files found.")

        log.info(f"Found {len(files)} RAN files for continuous training.")
        
        dfs = []

        for key in files:
            log.info(f"Reading file: {key}")
            csv_bytes = s3.get_object(Bucket=s3_bucket, Key=key)['Body'].read()
            df = pd.read_csv(
                io.BytesIO(csv_bytes),
                quotechar='"',
                delimiter=',',
                skipinitialspace=True,
                engine='python'
            )

            # If first row is repeated header, drop it
            if list(df.iloc[0]) == list(df.columns):
                df = df.iloc[1:]
            
            # Standardize column names
            df.columns = [
                "Cell ID", "Datetime", "Band", "Frequency", "UEs Usage", "Area Type", "Lat", "Lon", "City", "Adjacent Cells", 
                "RSRP", "RSRQ", "SINR", "Throughput (Mbps)", "Latency (ms)", "Max Capacity"
            ]

            dfs.append(df)

        # Concatenate all cleaned files
        combined_df = pd.concat(dfs, ignore_index=True)
        log.info(f"Total combined shape: {combined_df.shape}")
        log.info(f"Sample combined data:\n{combined_df.head(3).to_string(index=False)}")

        return combined_df
    
    # Load all RAN files from S3 for continuous training
    df = load_all_ran_files_from_s3(s3, s3_bucket, s3_key_prefix)


    '''
    # Read latest file
    files = sorted([o['Key'] for o in s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key_prefix).get('Contents', []) if "ran-combined-metrics" in o['Key']])
    if not files:
        raise FileNotFoundError("No RAN metrics files found.")
    latest_file = files[-1]
    log.info(f"Latest RAN metrics file selected: {latest_file}")

    # For Debug purpose reading the raw csv data from S3
    csv_bytes = s3.get_object(Bucket=s3_bucket, Key=latest_file)['Body'].read()
    # Debug print (optional) — shows first 5 lines of raw CSV
    print(csv_bytes.decode('utf-8').splitlines()[:5])

    # Read and clean headers
    df = pd.read_csv(
        io.BytesIO(s3.get_object(Bucket=s3_bucket, Key=latest_file)['Body'].read()), 
        quotechar='"',
        delimiter=',',          # Explicitly set delimiter
        skipinitialspace=True,
        engine='python'  # More flexible parser 
    )

    log.info(f"Initial data shape: {df.shape}")
    if list(df.iloc[0]) == list(df.columns):
        df = df.iloc[1:]
    df.columns = [
        "Cell ID", "Datetime", "Band", "Frequency", "UEs Usage", "Area Type", "Adjacent Cells", 
        "RSRP", "RSRQ", "SINR", "Throughput (Mbps)", "Latency (ms)", "Max Capacity"
    ]
    log.info(f"Cleaned data shape: {df.shape}")
    log.info(f"Raw data shape: {df.shape}")
    log.info(f"First 3 rows of raw data:\n{df.head(3).to_string(index=False)}")
    '''

    df.columns = df.columns.str.strip().str.replace('"', '')
    # Clean 'Adjacent Cells' column
    if 'Adjacent Cells' in df.columns:
        df['Adjacent Cells'] = df['Adjacent Cells'].astype(str).str.strip('"').str.strip()
    # (Optional: Print sample row for debug)
    print(df.head())

    # Drop invalid Cell IDs
    df = df[pd.to_numeric(df['Cell ID'], errors='coerce').notnull()]
    df['Cell ID'] = df['Cell ID'].astype(int)
    df['Max Capacity'] = pd.to_numeric(df['Max Capacity'], errors='coerce').round().astype('Int64')
    df['UEs Usage'] = pd.to_numeric(df['UEs Usage'], errors='coerce').fillna(0)
    df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')
    df.dropna(subset=['Datetime'], inplace=True)
    log.info(f"Data after basic cleaning: {df.shape}")
    log.info(f"Sample cleaned data:\n{df[['Cell ID', 'Datetime', 'Frequency', 'UEs Usage']].head(3).to_string(index=False)}")

    '''
    # Frequency processing
    def parse_frequency(freq):
        if isinstance(freq, str) and '-' in freq:
            try:
                start, end = map(int, freq.split('-'))
                return (start + end) / 2
            except:
                return 0
        try:
            return float(freq)
        except:
            return 0
    df['Frequency'] = df['Frequency'].apply(parse_frequency).fillna(0)
    log.info("Frequency column processed.")
    '''
    
    # Fill missing values
    df['Band'] = df['Band'].fillna("Unknown Band").astype(str)
    df['Area Type'] = df['Area Type'].fillna("Unknown Area").astype(str)

    '''
    # Derived features
    df['Hour'] = df['Datetime'].dt.hour
    df['DayOfWeek'] = df['Datetime'].dt.day_name()
    df['Weekend'] = df['DayOfWeek'].isin(['Saturday', 'Sunday']).astype(int)
    df['Date'] = df['Datetime'].dt.strftime('%m-%d-%Y')  # MM-DD-YYYY
    '''

    # Derive extra temporal features first
    #df['Datetime'] = pd.to_datetime(df['Datetime'])
    # Normalize datetime precision to seconds (or minutes if preferred)
    df['Datetime'] = pd.to_datetime(df['Datetime']).dt.floor('S')

    df['Hour'] = df['Datetime'].dt.hour
    df['Day'] = df['Datetime'].dt.day
    df['Month'] = df['Datetime'].dt.month
    df['Weekday'] = df['Datetime'].dt.weekday
    df['DayOfWeek'] = df['Datetime'].dt.day_name()
    df['Weekend'] = df['Weekday'].isin([5, 6])  # Saturday/Sunday
    df['Date'] = df['Datetime'].dt.date  # Needed for grouping
    # Create full Timestamp for grouping
    #df['Timestamp'] = pd.to_datetime(df['Date'].astype(str)) + pd.to_timedelta(df['Hour'], unit='h')
    log.info(f"Sample after feature derivation:\n{df[['Cell ID', 'Datetime', 'Date', 'Hour', 'Day', 'Month', 'Weekday',  'DayOfWeek', 'Weekend']].head(3).to_string(index=False)}")

    # Group by Cell ID + Frequency + Hour + Date
    log.info("Grouping by ['Cell ID', 'Datetime', 'Frequency']")
    grouped = df.groupby(['Cell ID', 'Datetime', 'Frequency']).agg({
        'UEs Usage': 'sum',
        'DayOfWeek': 'first',
        'Weekend': 'first'
    }).reset_index()
    '''
    log.info("Grouping by ['Cell ID', 'Frequency', 'Hour', 'Date']")
    grouped = df.groupby(['Cell ID', 'Frequency', 'Hour', 'Date']).agg({
        'UEs Usage': 'sum',
        'DayOfWeek': 'first',
        'Weekend': 'first'
    }).reset_index()
    '''
    log.info(f"Grouped data shape: {grouped.shape}")
    log.info(f"Sample grouped data:\n{grouped.head(3).to_string(index=False)}")

    '''
    # Merge back other time features (Day, Month, Weekday)
    grouped['Day'] = pd.to_datetime(grouped['Date']).dt.day
    grouped['Month'] = pd.to_datetime(grouped['Date']).dt.month
    grouped['Weekday'] = pd.to_datetime(grouped['Date']).dt.weekday
    grouped['Date'] = grouped['Datetime'].dt.strftime('%m-%d-%Y')
    grouped['Hour'] = grouped['Datetime'].dt.hour
    '''

    grouped['Date'] = grouped['Datetime'].dt.strftime('%m-%d-%Y')
    grouped['Hour'] = grouped['Datetime'].dt.hour
    grouped['Day'] = grouped['Datetime'].dt.day
    grouped['Month'] = grouped['Datetime'].dt.month
    grouped['Weekday'] = grouped['Datetime'].dt.weekday
    #grouped['Datetime'] = grouped['Datetime'].astype('int64') // 10**9  # seconds
    #grouped['Datetime'] = pd.to_datetime(grouped['Datetime'], errors='coerce')
    #grouped['Datetime_ts'] = grouped['Datetime'].astype('int64') // 10**9


    #grouped['Timestamp'] = pd.to_datetime(grouped['Date']).astype(np.int64) // 10**9  # UNIX timestamp

    # Traffic class label
    grouped['Traffic Class'] = (grouped['UEs Usage'] >= 40).astype(int)
    

    # One-hot encode DayOfWeek (as boolean flags)
    grouped = pd.get_dummies(grouped, columns=['DayOfWeek'], prefix='DayOfWeek')


    # --- One-hot encode top-N adjacent cells ---
    N = 10  # Number of top adjacent cells to encode
    if 'Adjacent Cells' in df.columns:
        # Flatten all adjacent cells into a list
        all_adjacent = df['Adjacent Cells'].dropna().str.split(',').explode().str.strip()
        top_adjacent = all_adjacent.value_counts().nlargest(N).index.tolist()
        log.info(f"Top-{N} most frequent adjacent cells: {top_adjacent}")

        # Create binary flags for top-N adjacent cells
        for adj in top_adjacent:
            col_name = f'adj_cell_{adj}'
            df[col_name] = df['Adjacent Cells'].apply(lambda x: int(adj in x.split(',')) if pd.notnull(x) else 0)

    # Ensure both 'Datetime' columns are in datetime64[ns] and aligned to second resolution BEFORE merging
    df['Datetime'] = pd.to_datetime(df['Datetime']).dt.floor('S')
    grouped['Datetime'] = pd.to_datetime(grouped['Datetime']).dt.floor('S')

    # Prepare the one-hot encoded adjacent cell flags
    adj_cols = [f'adj_cell_{adj}' for adj in top_adjacent]
    adj_encoded_df = df.groupby(['Cell ID', 'Datetime', 'Frequency'])[adj_cols].max().reset_index()

    # --- DEBUG: Check datatypes before merge ---
    log.info(f"[Before merge] adj_encoded_df['Datetime'] dtype: {adj_encoded_df['Datetime'].dtype}")
    log.info(f"[Before merge] grouped['Datetime'] dtype: {grouped['Datetime'].dtype}")

    # Ensure consistency in 'Datetime' type for adj_encoded_df as well
    adj_encoded_df['Datetime'] = pd.to_datetime(adj_encoded_df['Datetime']).dt.floor('S')

    # Now safely merge
    grouped = grouped.merge(adj_encoded_df, on=['Cell ID', 'Datetime', 'Frequency'], how='left')
    log.info(f"Shape after merging top-N adjacent cell features: {grouped.shape}")

    # --- FIX: Ensure 'Datetime' is proper before processing ---
    # Print sample raw 'Datetime' values
    log.info(f"Raw 'Datetime' values before coercion:\n{grouped['Datetime'].head()}")
    log.info(f"'Datetime' dtype before coercion: {grouped['Datetime'].dtype}")

    # Only convert if not already datetime64
    if not pd.api.types.is_datetime64_any_dtype(grouped['Datetime']):
        grouped['Datetime'] = pd.to_datetime(grouped['Datetime'], errors='coerce')

    # Optional: Drop rows where coercion failed
    if grouped['Datetime'].isna().any():
        log.warning("Some 'Datetime' values could not be converted and are NaT. Dropping those rows.")
        grouped = grouped.dropna(subset=['Datetime'])

    # Floor to seconds precision
    grouped['Datetime'] = grouped['Datetime'].dt.floor('S')

    # Derive Unix timestamp safely
    grouped['Datetime_ts'] = grouped['Datetime'].astype('int64') // 10**9

    # Confirm correctness
    log.info(f"Sample 'Datetime' values:\n{grouped['Datetime'].head().to_string(index=False)}")
    log.info(f"'Datetime' dtype: {grouped['Datetime'].dtype}")
    log.info(f"Sample 'Datetime_ts' values:\n{grouped['Datetime_ts'].head().to_string(index=False)}")


    # Define the one-hot column names
    adj_cols = [col for col in grouped.columns if col.startswith("adj_cell_")]
    log.info(f"Sample of one-hot adjacent cell columns BEFORE fillna:\n{grouped[adj_cols].head().to_string(index=False)}")

    # Fill missing values and convert to integers
    grouped[adj_cols] = grouped[adj_cols].fillna(0).astype(int)
    log.info(f"Sample AFTER fillna:\n{grouped[adj_cols].head().to_string(index=False)}")

    # Log and convert final 'Datetime' to timestamp
    log.info(f"'Datetime' dtype: {grouped['Datetime'].dtype}")
    log.info(f"Sample 'Datetime' values:\n{grouped['Datetime'].head().to_string(index=False)}")


    # Final features
    '''
    feature_cols = ['Cell ID', 'Datetime_ts', 'Datetime', 'Frequency', 'Hour', 'Weekend', 'Day', 'Month', 'Weekday', 'UEs Usage'] + \
                [col for col in grouped.columns if col.startswith('DayOfWeek_')] + \
                [col for col in grouped.columns if col.startswith('adj_cell_')]
    '''
    feature_cols = ['Cell ID', 'Datetime_ts', 'Datetime', 'Hour', 'Weekend', 'Day', 'Month', 'Weekday', 'UEs Usage'] + \
                [col for col in grouped.columns if col.startswith('DayOfWeek_')]
    X = grouped[feature_cols]
    #X = pd.get_dummies(X, columns=['Frequency'], prefix='Freq')

    print("Current columns in grouped:", grouped.columns.tolist())

    
    # Drop only if columns exist to avoid KeyError
    columns_to_drop = [col for col in ["Datetime"] if col in X.columns]
    X = X.drop(columns=columns_to_drop)
    print("After dropping columns:", X.columns.tolist())
    

    y_class = grouped["Traffic Class"]
    y_usage = grouped["UEs Usage"]

    log.info(f"Feature columns: {feature_cols}")
    log.info(f"Final training feature set shape: {X.shape}")
    log.info(f"Training features sample:\n{X.head(3).to_string(index=False)}")
    log.info(f"Training target (UEs Usage) sample:\n{y_usage.head(3).tolist()}")
    log.info(f"Training target (Traffic Class) sample:\n{y_class.head(3).tolist()}")

    # Train-test split
    X_train, X_test, y_train_usage, y_test_usage, y_train_class, y_test_class = train_test_split(
        X, y_usage, y_class, test_size=0.2, random_state=42
    )

    # Train models
    regressor = RandomForestRegressor(n_estimators=100, random_state=42)
    regressor.fit(X_train, y_train_usage)

    classifier = RandomForestClassifier(n_estimators=100, random_state=42)
    classifier.fit(X_train, y_train_class)

    log.info("RandomForest models trained successfully.")

    '''
    # Save models
    os.makedirs("/tmp", exist_ok=True)
    reg_path = f"/tmp/traffic_regressor_model.joblib"
    clf_path = f"/tmp/traffic_classifier_model.joblib"
    joblib.dump(regressor, reg_path)
    joblib.dump(classifier, clf_path)

    # Output artifacts
    output_traffic_regressor_model.path = reg_path
    output_traffic_classifier_model.path = clf_path

    log.info("Training completed and models saved.")
    '''

    # Save
    os.makedirs("/tmp", exist_ok=True)
    regressor_model_path = "/tmp/traffic_regressor_model.joblib"
    classifier_model_path = "/tmp/traffic_classifier_model.joblib"
    #regressor_model_path = "/tmp/traffic_regressor_model_{timestamp_str}.joblib"
    #classifier_model_path = "/tmp/traffic_classifier_model_{timestamp_str}.joblib"    
    joblib.dump(regressor, regressor_model_path)
    joblib.dump(classifier, classifier_model_path)

    log.info("Uploading models to S3...")
    with open(regressor_model_path, 'rb') as r_file:
        s3.upload_fileobj(r_file, s3_bucket, f'{s3_key_prefix}/models/traffic_regressor_model.joblib')
    with open(classifier_model_path, 'rb') as c_file:
        s3.upload_fileobj(c_file, s3_bucket, f'{s3_key_prefix}/models/traffic_classifier_model.joblib')

    log.info(f"Uploaded regressor model: {s3_key_prefix}/models/traffic_regressor_model.joblib")
    log.info(f"Uploaded classifier model: {s3_key_prefix}/models/traffic_classifier_model.joblib")

    '''
    log.info("Uploading models to S3...")
    with open(regressor_model_path, 'rb') as r_file:
        s3.upload_fileobj(r_file, s3_bucket, f'{s3_key_prefix}/models/traffic_regressor_model_{timestamp_str}.joblib')
    with open(classifier_model_path, 'rb') as c_file:
        s3.upload_fileobj(c_file, s3_bucket, f'{s3_key_prefix}/models/traffic_classifier_model_{timestamp_str}.joblib')

    log.info(f"Uploaded regressor model: {s3_key_prefix}/models/traffic_regressor_model_{timestamp_str}.joblib")
    log.info(f"Uploaded classifier model: {s3_key_prefix}/models/traffic_classifier_model_{timestamp_str}.joblib")
    '''

    # Output regressor to pipeline output
    with open(regressor_model_path, 'rb') as f:
        with open(output_traffic_regressor_model.path, 'wb') as out_f:
            out_f.write(f.read())

    with open(classifier_model_path, 'rb') as f:
        with open(output_traffic_classifier_model.path, 'wb') as out_f:
            out_f.write(f.read())

    log.info("Traffic predictor models saved and uploaded successfully.")


#
# GenAI Anomaly detection component
# ===================================================
@component(
    base_image="python:3.9",
    packages_to_install=[
        "openai", "langchain", "sqlalchemy", "langchain-community", "sentence-transformers", "faiss-cpu", "pymysql", "boto3", "pandas", "requests",
        "click>=8.0.0,<9",
        "docstring-parser>=0.7.3,<1",
        "google-api-core!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.0,<3.0.0dev,>=1.31.5",
        "google-auth>=1.6.1,<3",
        "google-cloud-storage>=2.2.1,<4",
        "kubernetes>=8.0.0,<31",
        "protobuf>=4.21.1,<5",
        "tabulate>=0.8.6,<1",
        "pyarrow", # Added for potential future Parquet support or general dataframe ops
        "fastparquet" # Added for potential future Parquet support
    ]
)
def genai_anomaly_detection(
    s3_bucket: str,
    s3_key_prefix: str, # This is an input for the component
    s3_endpoint: str,
    aws_access_key: str,
    aws_secret_key: str,
    model_api_key: str,
    model_api_url: str,
    model_api_name: str,
    db_host: str,
    db_user: str,
    db_pwd: str,
    db_name: str,
    ran_metrics_path: Input[Dataset] # This is the current batch CSV from S3
):
    import requests
    import logging
    import pandas as pd
    import boto3
    import os
    import re
    from botocore.config import Config
    from langchain_community.llms import VLLMOpenAI
    from langchain.text_splitter import RecursiveCharacterTextSplitter
    from langchain_community.embeddings import HuggingFaceEmbeddings
    from langchain_community.vectorstores import FAISS
    from langchain.chains import RetrievalQA
    from langchain.tools import Tool
    from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Text, DateTime
    from sqlalchemy.sql import select, desc
    from datetime import datetime, timedelta
    from io import StringIO # Import StringIO for CSV handling
    import csv

    # Logging setup
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] - %(message)s')
    log = logging.getLogger()

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    log.info("GenAI anomaly detection started")

    # Setup AWS
    os.environ.update({
        'AWS_ACCESS_KEY_ID': aws_access_key,
        'AWS_SECRET_ACCESS_KEY': aws_secret_key
    })
    s3 = boto3.client('s3', endpoint_url=s3_endpoint)
    log.info("AWS credentials and environment variables configured.")

    # Read CSV contents as string from ran_metrics_path artifact (this is the current batch)
    with open(ran_metrics_path.path, "r") as f:
        current_batch_csv_data = f.read()

    log.info('Connect to database')
    engine = create_engine('mysql+pymysql://%s:%s@%s/%s' % (db_user, db_pwd, db_host, db_name), echo=True)
    metadata = MetaData()
    events = Table(
        'events', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('creation_date', DateTime, nullable=False),
        Column('event', Text, nullable=False),
        Column('data', Text, nullable=True)
    )

    # insert event in th DB
    def insert_event_db(event_text, event_data, raw_csv_data):
        with engine.begin() as conn:
            conn.execute(events.insert().values(
                creation_date=datetime.utcnow(),
                event=event_text,
                data=raw_csv_data
            ))

    # --- START OF MOVED AND MODIFIED FUNCTIONS ---

    # NEW: Function to retrieve historical data from S3
    def get_historical_data(s3_client_obj, bucket_name, key_prefix, cell_id, band, current_record_datetime, num_rows=3):
        """
        Retrieves the 'num_rows' most recent historical records for a given Cell ID and Band
        from S3, excluding data newer than current_record_datetime.
        """
        historical_records = []
        # Construct a flexible prefix for listing objects related to this Cell ID and Band
        # Assumes s3_key_prefix for the streaming component creates paths like:
        # {s3_key_prefix}/ran-combined-metrics/ran-combined-metrics_{timestamp}.csv
        common_search_prefix = f"{key_prefix}/ran-combined-metrics/"

        try:
            # List all objects under the common prefix
            response = s3_client_obj.list_objects_v2(Bucket=bucket_name, Prefix=common_search_prefix)
            if 'Contents' not in response:
                log.info(f"No objects found under {common_search_prefix} for historical data.")
                return pd.DataFrame()

            # Filter for CSV files and sort by LastModified (most recent first)
            # This is simplified; robust time-based filtering might require parsing timestamps from file names
            sorted_objects = sorted(
                [obj for obj in response['Contents'] if obj['Key'].endswith('.csv')],
                key=lambda x: x['LastModified'],
                reverse=True
            )

            # Define column names for parsing historical data
            history_column_names = [
                "Cell ID", "Datetime", "Band", "Frequency", "UEs Usage", "Area Type", "Lat", "Lon", "City", "Adjacent Cells",
                "RSRP", "RSRQ", "SINR", "Throughput (Mbps)", "Latency (ms)", "Max Capacity"
            ]

            # Iterate through historical files to find relevant data
            found_count = 0
            for obj in sorted_objects:
                if found_count >= num_rows:
                    break # Stop if we have enough historical records

                obj_key = obj['Key']
                # Skip the current file being processed in the pipeline run if it's found in history list
                # This requires a unique way to identify the current file. For now, we assume
                # the current batch CSV won't be listed as historical unless it's very recent.
                # A better approach for excluding current file might be to check its full key.
                
                log.debug(f"Checking historical file: {obj_key}")

                # Read the CSV content
                csv_obj_response = s3_client_obj.get_object(Bucket=bucket_name, Key=obj_key)
                body = csv_obj_response['Body'].read().decode('utf-8')

                # Read into a DataFrame, ensuring correct column names are used.
                # `header=0` means the first row is the header. If your CSVs have a header, this is correct.
                # If they don't, set `header=None`.
                temp_df = pd.read_csv(StringIO(body), names=history_column_names, header=0)

                # Filter for the specific Cell ID and Band, and data strictly older than the current record
                temp_df['Datetime'] = pd.to_datetime(temp_df['Datetime'])
                
                filtered_history = temp_df[
                    (temp_df['Cell ID'] == cell_id) &
                    (temp_df['Band'] == band) &
                    (temp_df['Datetime'] < current_record_datetime) # Only include data strictly older
                ].sort_values(by='Datetime', ascending=False) # Ensure latest historical are first from this file

                # Add to our collected historical records
                for _, row in filtered_history.iterrows():
                    if found_count < num_rows:
                        historical_records.append(row.to_dict())
                        found_count += 1
                    else:
                        break

            # Convert to DataFrame and return the top `num_rows`
            if historical_records:
                final_history_df = pd.DataFrame(historical_records, columns=history_column_names)
                final_history_df['Datetime'] = pd.to_datetime(final_history_df['Datetime'])
                # Sort from oldest to newest for chronological presentation in the prompt
                return final_history_df.sort_values(by='Datetime', ascending=True)
            else:
                return pd.DataFrame(columns=history_column_names) # Return empty DataFrame if no history

        except Exception as e:
            log.error(f"Error retrieving historical data for Cell ID {cell_id}, Band {band}: {e}", exc_info=True)
            return pd.DataFrame(columns=history_column_names)

    def download_index_from_s3(bucket, prefix, local_dir):
        os.makedirs(local_dir, exist_ok=True)
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in response.get('Contents', []):
            s3_key = obj['Key']
            local_path = os.path.join(local_dir, os.path.relpath(s3_key, prefix))
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3.download_file(bucket, s3_key, local_path)

    def get_chain():
        log.info("VLLM inference load model %s" % model_api_name)
        llm = VLLMOpenAI(
            openai_api_key=model_api_key,
            openai_api_base=model_api_url,
            model_name=model_api_name,
            max_tokens=2000,
            temperature=0.1, # Keep temperature low for deterministic output
        )
        embedding = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")

        log.info("Loading FAISS index from S3")
        download_index_from_s3(s3_bucket, "faiss_index", "/tmp/faiss_index")
        vectordb = FAISS.load_local("/tmp/faiss_index", embeddings=embedding, allow_dangerous_deserialization=True)

        retriever = vectordb.as_retriever()
        qa_chain = RetrievalQA.from_chain_type(llm=llm, chain_type="stuff", retriever=retriever, return_source_documents=True)
        log.info("Chain created")
        return qa_chain

    # --- MOVED: verify_anomaly_block function definition ---
    def verify_anomaly_block(text_block):
        """
        Verify that the values in the anomaly report actually violate thresholds defined in the prompt.
        If they do not, this is likely a hallucination.
        NOTE: This function checks for individual threshold violations based on *parsed numbers from LLM output*.
        It does not re-calculate from raw data, nor does it fully verify complex multi-condition rules like "Cell Outage"
        unless the LLM explicitly reports all conditions and those are parsed.
        """
        sinr_match = re.search(r"SINR.*?(-?\d+\.?\d*)\s*dB", text_block)
        rsrp_match = re.search(r"RSRP.*?(-?\d+\.?\d*)\s*dBm", text_block)
        prb_match = re.search(r"PRB Utilization.*?(\d+\.?\d*)\s*%", text_block)
        
        # Note: You might need to add regex for Throughput and UEs if LLM explicitly states their anomalous values.
        # However, the core purpose of this function is to prevent LLM from saying "Anomaly" when numbers don't match.

        sinr_val = float(sinr_match.group(1)) if sinr_match else None
        rsrp_val = float(rsrp_match.group(1)) if rsrp_match else None
        prb_val = float(prb_match.group(1)) if prb_match else None

        log.debug(f"Parsed values for verification - SINR: {sinr_val}, RSRP: {rsrp_val}, PRB: {prb_val}")

        # Check against prompt's simple thresholds (Rules 1, 2, 3)
        if sinr_val is not None and sinr_val < 0:
            log.debug(f"SINR {sinr_val} violates < 0 dB threshold.")
            return True
        if rsrp_val is not None and rsrp_val < -110:
            log.debug(f"RSRP {rsrp_val} violates < -110 dBm threshold.")
            return True
        if prb_val is not None and prb_val > 95:
            log.debug(f"PRB Utilization {prb_val} violates > 95.0% threshold.")
            return True
        
        # If the LLM output explicitly mentions "Cell Outage" and lists all 5 conditions,
        # you would need additional regex and logical AND checks here.
        # Example for Cell Outage check if it's reported by LLM:
        # if "Cell outage" in text_block:
        #    ue_usage_match = re.search(r"UEs Usage.*?=\s*(\d+)", text_block)
        #    throughput_match = re.search(r"Throughput.*?=\s*(\d+\.?\d*)\s*Mbps", text_block)
        #    # ... and so on for SINR, RSRP, RSRQ from the anomaly text
        #    # Then check all conditions:
        #    if (ue_usage_match and int(ue_usage_match.group(1)) == 0) and \
        #       (throughput_match and float(throughput_match.group(1)) == 0) and \
        #       ... (other checks) ... :
        #        log.debug("Cell Outage conditions met based on LLM's anomaly text.")
        #        return True


        # If no clear threshold violation based on the *numbers reported in the anomaly text* is found
        return False
    # --- END OF MOVED FUNCTIONS ---


    # Convert current batch CSV string to DataFrame
    df_current_batch = pd.read_csv(StringIO(current_batch_csv_data))

    # Log band distribution before filtering
    log.info(f"Unique bands before filtering: {df_current_batch['Band'].unique().tolist()}")
    log.info(f"Row count before filtering: {df_current_batch.shape[0]}")

    # Filter by valid bands (apply to current batch)
    VALID_BANDS = ['Band 29', 'Band 26', 'Band 71', 'Band 66']
    df_current_batch = df_current_batch[df_current_batch['Band'].isin(VALID_BANDS)]

    # Log band distribution after filtering
    log.info(f"Unique bands after filtering: {df_current_batch['Band'].unique().tolist()}")
    log.info(f"Row count after filtering: {df_current_batch.shape[0]}")

    # Prepare band_map for the prompt
    band_map_str = df_current_batch[['Cell ID', 'Band']].drop_duplicates().head(100).to_csv(index=False)

    # --- UPDATED PROMPT TEMPLATE ---
    prompt_template = """
    You are a Radio Access Network (RAN) engineer and telecom operations expert with access to Baicells BaiBNQ gNodeB technical documentation.

    Each row in the dataset includes the following columns:

    - Cell ID: Unique identifier of the cell site  
    - Datetime: Time of metric capture  
    - Band: Frequency band used (e.g., 71, 66, 26, 29)  
    - Frequency: Operating frequency range (e.g., 1700-2100)
    - UEs Usage: Number of connected user equipment (UEs)  
    - Area Type: Type of area (e.g., commercial, industrial, rural)
    - Lat: Latitude
    - Lon: Longitude
    - City: City where the cell is located
    - Adjacent Cells: List of adjacent cell IDs
    - RSRP (dBm): Reference Signal Received Power  
    - RSRQ (dB): Reference Signal Received Quality  
    - SINR (dB): Signal to Interference plus Noise Ratio  
    - Throughput (Mbps): Data throughput
    - Latency (ms): Latency
    - Max Capacity: Maximum UE capacity for the cell  

    Your task is to analyze the provided 5G RAN performance data **strictly using the anomaly thresholds below**.

    ---

    ### RULE CHECKLIST (Apply each to the CURRENT_ROW only)

    1. **High PRB Utilization** - Formula: PRB Utilization = (UEs Usage / Max Capacity) × 100  
    - Threshold: Anomaly if **PRB Utilization > 95.0%**
    - Example:  
        - UEs = 51, Max Capacity = 62  
        - PRB Utilization = (51 / 62) × 100 = 82.26% -> Not an anomaly  
        - Do not report unless PRB Utilization > 95.0%

    2. **Low RSRP**: Anomaly if RSRP < -110 dBm  
    3. **Low SINR**: Anomaly if SINR < 0 dB  
    4. **Throughput drop**: Anomaly if current Throughput is more than 50% drop vs. average of 3 prior rows (same Cell ID + Band). **Only apply this rule if at least 3 rows are available in HISTORICAL_DATA.**
       - Calculation: `Prior Avg Throughput = (Prior_Throughput_1 + Prior_Throughput_2 + Prior_Throughput_3) / 3`
       - Anomaly if `CURRENT_ROW.Throughput (Mbps) < 0.5 * Prior Avg Throughput`
    5. **UEs spike/drop**: Anomaly if current UEs Usage is >50% change vs. average of 3 prior rows (same Cell ID + Band). **Only apply this rule if at least 3 rows are available in HISTORICAL_DATA.**
       - Calculation: `Prior Avg UEs = (Prior_UEs_1 + Prior_UEs_2 + Prior_UEs_3) / 3`
       - Anomaly if `(abs(CURRENT_ROW.UEs Usage - Prior Avg UEs) / Prior Avg UEs) > 0.5`

    6. **Cell outage**: Must meet **ALL** conditions for the CURRENT_ROW:  
    - UEs Usage = 0  
    - Throughput = 0 Mbps  
    - SINR <= -10  
    - RSRP <= -120  
    - RSRQ <= -20  

    ---

    ###  DO NOT

    - Report anomalies unless numeric thresholds are **clearly violated**.
    - Flag PRB Utilization if **<= 95.0%**.  
    - Say "low SINR" unless **< 0 dB**.  
    - Fabricate historical patterns or infer behavior from other Cell IDs.  
    - Report “suspected” issues or use assumptions — rely only on direct numeric evidence.  
    - Repeat rule definitions in your answer.
    - **Crucially: NEVER include explanations, calculations, or additional text beyond the required output format. ONLY output the exact required format.**
    - **Do not apply Throughput drop or UEs spike/drop rules if HISTORICAL_DATA is incomplete (less than 3 rows).**

    ---

    ### ANALYSIS INSTRUCTIONS

    - Analyze the **CURRENT_ROW** data.
    - Evaluate each rule in the checklist above **strictly**.
    - For PRB Utilization, show the full calculation and threshold comparison step **ONLY IF ANOMALY DETECTED**. Otherwise, **DO NOT** show calculations or explanations.
    - For Throughput drop and UEs spike/drop, use the provided **HISTORICAL_DATA** for comparison. If **HISTORICAL_DATA** has less than 3 rows, explicitly state that these rules are skipped because of insufficient historical data, but do not make this part of the final anomaly report.

    ---

    ### FORMAT REQUIREMENTS (STRICT - NO ADDITIONAL TEXT, NO DEVIATION)

    If **any** anomaly is found, respond **EXACTLY** like this (include START_EVENT/END_EVENT tags, no extra spaces or characters outside these lines):

    **** START_EVENT **** ANOMALY_DETECTED  
    1. Cell ID {current_cell_id}, Band {current_band}  
    - <Metric and reason that violated the threshold>  
    - Recommended fix: Refer to Baicells documentation (Section X.X, Page Y)  
    **** END_EVENT ****

    If the row is **not** anomalous, respond **EXACTLY** with:

    NO_ANOMALY

    ---

    Below are the valid Cell ID and Band combinations for reference:  
    {band_map}

    Below is the current RAN metric row to analyze:

    CURRENT_ROW:  
    {current_chunk}

    Below are the 3 prior historical RAN metric rows for comparison (if available and relevant for rules 4 & 5). These rows are for the same Cell ID and Band as CURRENT_ROW. If less than 3 rows are provided, rules 4 & 5 cannot be applied.

    HISTORICAL_DATA:  
    {history_chunk}
    """.strip()

    qa_chain = get_chain()
    log.info("Execute anomaly detection")

    # STEP 3: Iterate through each row of the current batch for LLM analysis
    results = [] # To store LLM responses
    anomaly_results_df = pd.DataFrame(columns=[
        "Cell ID", "Band", "Datetime", "LLM_Response", "Is_Anomaly", "Parsed_Anomaly_Reason"
    ])

    # Convert the current batch DataFrame to a list of dictionaries for easier row processing
    records_to_analyze = df_current_batch.to_dict(orient='records')

    for record_dict in records_to_analyze:
        current_cell_id = record_dict["Cell ID"]
        current_band = record_dict["Band"]
        current_datetime = datetime.strptime(record_dict["Datetime"], "%Y-%m-%d %H:%M:%S")

        # Format the current record as a single-row CSV for the LLM
        current_chunk_io = StringIO()
        pd.DataFrame([record_dict]).to_csv(current_chunk_io, index=False, header=True, quoting=csv.QUOTE_ALL)
        current_chunk_str = current_chunk_io.getvalue()

        # --- Fetch historical data for this specific Cell ID and Band ---
        # Pass `s3` (your boto3 client) and `s3_key_prefix` to the function
        history_df = get_historical_data(s3, s3_bucket, s3_key_prefix, current_cell_id, current_band, current_datetime, num_rows=3)

        history_chunk_io = StringIO()
        if not history_df.empty:
            history_df.to_csv(history_chunk_io, index=False, header=True, quoting=csv.QUOTE_ALL)
        else:
            # Provide explicit message for LLM if no history
            history_chunk_io.write("No historical data available for this Cell ID and Band.")
        history_chunk_str = history_chunk_io.getvalue()

        # Format the prompt with current and historical data
        prompt = prompt_template.format(
            current_cell_id=current_cell_id,
            current_band=current_band,
            band_map=band_map_str, # Use the `band_map_str` derived from the current batch
            current_chunk=current_chunk_str,
            history_chunk=history_chunk_str
        )

        log.info(f"Running LLM for Cell ID: {current_cell_id}, Band: {current_band}")
        log.debug(f"Full Prompt for {current_cell_id}, {current_band}:\n{prompt}") # Log full prompt for debugging
        response = qa_chain.invoke(prompt)
        result_text = response['result']

        log.info(f"LLM Raw Response for Cell ID {current_cell_id}, Band {current_band}:\n{result_text}\n")

        # --- Post-LLM Parsing Logic (Improved) ---
        is_anomaly_detected = False
        parsed_anomaly_reason = "N/A"
        
        match = re.search(r"\*\*\*\* START_EVENT \*\*\*\*(.*?)\*\*\*\* END_EVENT \*\*\*\*", result_text, re.DOTALL)
        if match:
            anomaly_block_content = match.group(1).strip()
            if "ANOMALY_DETECTED" in anomaly_block_content:
                # Validate the anomaly based on parsed values if possible
                if verify_anomaly_block(anomaly_block_content): # Now `verify_anomaly_block` is defined!
                    is_anomaly_detected = True
                    parsed_anomaly_reason = anomaly_block_content
                    log.info(f"Anomaly validated and detected for Cell ID {current_cell_id}, Band {current_band}")
                else:
                    log.warning(f"Anomaly block failed validation for Cell ID {current_cell_id}, Band {current_band}: Hallucination suspected.\n{anomaly_block_content}")
                    # Decide if you still want to mark as anomaly or as false positive
                    is_anomaly_detected = False # Treat as false positive if validation fails
                    parsed_anomaly_reason = f"HALLUCINATION_SUSPECTED: {anomaly_block_content}"
            else: # If START/END tags but no ANOMALY_DETECTED (e.g., NO_ANOMALY wrapped)
                 log.warning(f"Unexpected content within START/END_EVENT tags (not ANOMALY_DETECTED) for Cell ID {current_cell_id}, Band {current_band}. Output:\n{result_text}")
                 is_anomaly_detected = False
                 parsed_anomaly_reason = f"UNEXPECTED_STRUCTURE: {result_text}"
        elif result_text.strip() == "NO_ANOMALY":
            is_anomaly_detected = False
            parsed_anomaly_reason = "NO_ANOMALY"
            log.info(f"No anomaly detected for Cell ID {current_cell_id}, Band {current_band}")
        else:
            log.warning(f"WARNING: Unrecognized LLM output format for Cell ID {current_cell_id}, Band {current_band}. Treating as NO_ANOMALY for safety. Output:\n{result_text}")
            is_anomaly_detected = False # Default to no anomaly for unknown format
            parsed_anomaly_reason = f"UNRECOGNIZED_FORMAT: {result_text}"

        # Append result to DataFrame
        new_row_df = pd.DataFrame([{
            "Cell ID": current_cell_id,
            "Band": current_band,
            "Datetime": record_dict["Datetime"], # Use original string datetime
            "LLM_Response": result_text,
            "Is_Anomaly": is_anomaly_detected,
            "Parsed_Anomaly_Reason": parsed_anomaly_reason
        }])
        anomaly_results_df = pd.concat([anomaly_results_df, new_row_df], ignore_index=True)


    # Final output of the component: Save the anomaly detection results
    anomaly_output_key = f"anomaly_results/{timestamp_str}_anomalies.csv" # A new path for anomaly results
    anomaly_output_uri = f"s3://{s3_bucket}/{anomaly_output_key}"

    # Ensure output directory exists for the artifact (if ran_metrics is an Output[Dataset])
    # This component's output should be an artifact that contains the anomaly results
    # For now, let's just save to S3
    anomaly_output_buffer = StringIO()
    anomaly_results_df.to_csv(anomaly_output_buffer, index=False, quoting=csv.QUOTE_ALL)
    s3.put_object(Bucket=s3_bucket, Key=anomaly_output_key, Body=anomaly_output_buffer.getvalue())

    log.info(f"\nLLM anomaly analysis results saved to {anomaly_output_uri}")

    # You could also optionally create an Output[Dataset] for these results
    # with open(metrics_output.path, 'w') as f:
    #     anomaly_results_df.to_csv(f, index=False, header=True)


    log.info("Anomaly detection component finished.")



    '''
    for result in results:
        if 'ANOMALY_DETECTED' in result:  #  Correct detection string
            # Remove the ANOMALY_DETECTED prefix from the full result
            cleaned_result = result.replace("ANOMALY_DETECTED", "").strip()

            # Extract only the START_EVENT block
            match = re.search(r'\*\*\*\* START_EVENT \*\*\*\*(.*?)\*\*\*\* END_EVENT \*\*\*\*', cleaned_result, re.DOTALL)

            if match:
                anomaly_text = match.group(1).strip()
                anomaly_blocks.append(anomaly_text)
            else:
                log.warning("ANOMALY_DETECTED found, but START/END_EVENT block missing.")

    if anomaly_blocks:
        combined_anomalies = "\n\n".join(anomaly_blocks)
        log.info("Detected anomalies:\n%s" % combined_anomalies)
        insert_event_db(combined_anomalies, "auto_detected_anomaly", df.to_csv(index=False))
    else:
        log.info("No anomalies detected in any chunk.")
    '''

    '''
    anomaly_blocks = []
    for result in results:
        if 'ANOMALY_DETECT' in result:
            match = re.search(r'\*\*\*\* START_EVENT \*\*\*\*(.*?)\*\*\*\* END_EVENT \*\*\*\*', result, re.DOTALL)
            if match:
                anomaly_text = match.group(1).strip().replace('ANOMALY_DETECTED', '')
                anomaly_blocks.append(anomaly_text)

    if anomaly_blocks:
        combined_anomalies = "\n\n".join(anomaly_blocks)
        log.info("Detected anomalies:\n%s" % combined_anomalies)
        insert_event_db(combined_anomalies, "auto_detected_anomaly", df.to_csv(index=False))
    else:
        log.info("No anomalies detected in any chunk.")
    '''

    '''
    if 'ANOMALY_DETECT' in response['result']:
        log.info("Anamoly detected save event to the DB")
        # Extract text between the START_EVENT and END_EVENT markers
        match = re.search(r'\*\*\*\* START_EVENT \*\*\*\*(.*?)\*\*\*\* END_EVENT \*\*\*\*', response['result'], re.DOTALL)
        if match:
            extracted_event = match.group(1).strip().replace('ANOMALY_DETECTED', '')
            log.info("Extracted event: %s" % extracted_event)
            #insert_event_db(extracted_event, "somedata")
            insert_event_db(extracted_event, "auto_detected_anomaly", csv_data)
        else:
            log.error("No event block found.")

    log.info("Anomaly detection completed")
    '''

# ─────────────────────────────────────────────────────────────── #
# LSTM Prediction Component
'''
@component(
    base_image="python:3.9",
    packages_to_install=["boto3", "pandas", "numpy", "tensorflow", "scikit-learn"]
)
def predict_kpi_anomalies_component(
    s3_bucket: str,
    s3_key_prefix: str,
    s3_endpoint: str,
    aws_access_key: str,
    aws_secret_key: str,
    output_kpi_anomaly_predictions: Output[Dataset],
):
    import os, boto3, pandas as pd, numpy as np, io, tempfile
    from tensorflow.keras.models import load_model
    from sklearn.preprocessing import MinMaxScaler
    from datetime import datetime
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(levelname)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    log = logging.getLogger()

    os.environ.update({
        'AWS_ACCESS_KEY_ID': aws_access_key,
        'AWS_SECRET_ACCESS_KEY': aws_secret_key,
    })

    s3 = boto3.client('s3', endpoint_url=s3_endpoint)

    # Load LSTM model from S3
    model_key = f"{s3_key_prefix}/models/lstm_kpi_model.h5"
    with tempfile.NamedTemporaryFile(suffix=".h5") as tmp_model:
        tmp_model.write(s3.get_object(Bucket=s3_bucket, Key=model_key)['Body'].read())
        tmp_model.flush()
        model = load_model(tmp_model.name, compile=False)
        model.compile(optimizer='adam', loss='mse', metrics=['mae'])
    log.info("Model loaded and compiled successfully.")

    # Read latest combined metrics CSV
    files = sorted([
        o['Key'] for o in s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key_prefix).get('Contents', [])
        if "ran-combined-metrics" in o['Key']
    ])
    latest_file = files[-1]
    log.info(f"Reading data from: {latest_file}")

    column_names = [
        "CellID", "Timestamp", "Band", "Frequency", "AreaType", "AdjacentCells", 
        "RSRP", "RSRQ", "SINR", "Throughput", "Latency", "MaxCapacity", "UEsUsage"
    ]
    kpi_columns = ["RSRP", "RSRQ", "SINR", "Throughput", "Latency", "MaxCapacity"]

    df = pd.read_csv(io.BytesIO(s3.get_object(Bucket=s3_bucket, Key=latest_file)['Body'].read()), header=None, names=column_names)
    df[kpi_columns] = df[kpi_columns].replace(",", "", regex=True).astype(float)
    df = df.dropna(subset=kpi_columns)

    TIME_STEPS = 10
    cell_ids = df["CellID"].values[TIME_STEPS:]
    timestamps = df["Timestamp"].values[TIME_STEPS:]

    scaler = MinMaxScaler()
    data_scaled = scaler.fit_transform(df[kpi_columns])

    def create_sequences(data): 
        return np.array([data[i:i+TIME_STEPS] for i in range(len(data)-TIME_STEPS)])
    
    X = create_sequences(data_scaled)
    X_pred = model.predict(X)
    mse = np.mean(np.abs(X_pred - X), axis=(1, 2))

    anomaly_df = pd.DataFrame({
        "CellID": cell_ids,
        "Timestamp": timestamps,
        "anomaly_score": mse,
    })

    # Save to S3
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"{s3_key_prefix}/predictions/anomaly_{timestamp_str}.csv"
    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=anomaly_df.to_csv(index=False))
    log.info(f"Anomaly prediction uploaded to: s3://{s3_bucket}/{s3_key}")

    # Save to Kubeflow output artifact
    with open(output_kpi_anomaly_predictions.path, 'w') as f:
        f.write(anomaly_df.to_csv(index=False))
    log.info("Anomaly predictions written to Kubeflow output artifact.")
'''


'''
# ─────────────────────────────────────────────────────────────── #
# GenAI KPI + DU Forecast using MaaS Mistral LLM  Component

@component(
    base_image="python:3.9",
    packages_to_install=["pandas", "requests", "boto3"]
)
def genai_kpi_traffic_forecast_explainer_component(
    output_kpi_anomaly_predictions: Input[Dataset],
    output_traffic_predictions: Input[Dataset],
    du_metrics: Input[Dataset],
    mistral_api_url: str,
    mistral_api_key: str,
    output_explanation: Output[Dataset]
):
    import pandas as pd
    import re
    import requests

    def call_mistral(prompt: str, api_key: str, url: str) -> str:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "mistral-7b-instruct",  # or whatever your endpoint expects
            "messages": [{"role": "user", "content": prompt}]
        }
        try:
            res = requests.post(url, headers=headers, json=payload, timeout=10)
            res.raise_for_status()
            return res.json().get("choices", [{}])[0].get("message", {}).get("content", "No response")
        except Exception as e:
            return f"Error calling Mistral API: {e}"

    # Load CSVs
    kpi_df = pd.read_csv(output_kpi_anomaly_predictions.path)
    traffic_df = pd.read_csv(output_traffic_predictions.path)
    du_df = pd.read_csv(du_metrics.path)

    # Normalize column names
    du_df.columns = [
        "CellID", "Timestamp", "CPU (%)", "Memory (MB)",
        "Disk Space (GB)", "RTT (ms)", "Temperature (C)", "Power Usage (W)"
    ]
    def normalize(col):
        return re.sub(r'[^a-zA-Z0-9]', '', col).lower()

    kpi_df.columns = [normalize(c) for c in kpi_df.columns]
    traffic_df.columns = [normalize(c) for c in traffic_df.columns]
    du_df.columns = [normalize(c) for c in du_df.columns]

    df = pd.merge(kpi_df, traffic_df, on=["cellid", "timestamp"], how="outer")
    df = pd.merge(df, du_df, on=["cellid", "timestamp"], how="outer")
    df.fillna("-", inplace=True)

    summaries = []
    for _, row in df.iterrows():
        prompt = f"""
You are a Radio Access Network (RAN) engineer and telecom operations expert. Analyze the following cell site data:

CellID: {row.get('cellid')}
KPI Anomaly Score: {row.get('anomalyscore', 'N/A')}
Predicted Traffic Load: {row.get('predictedtraffic', 'N/A')}
DU CPU Usage: {row.get('cpu', 'N/A')}%
Memory Usage: {row.get('memorymb', 'N/A')} MB
Disk Usage: {row.get('diskspacegb', 'N/A')} GB
RTT: {row.get('rttms', 'N/A')} ms
Temperature: {row.get('temperaturec', 'N/A')} °C
Power Usage: {row.get('powerusagew', 'N/A')} W

Based on the anomaly score, traffic prediction, and DU metrics, provide a human-readable summary of this cell's current and forecasted status. Mention if intervention is needed or if the cell is stable.
""".strip()
        summary = call_mistral(prompt, mistral_api_key, mistral_api_url)
        summaries.append({
            "CellID": row.get("cellid"),
            "GenAI Summary": summary
        })

    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv(output_explanation.path, index=False)
'''


# ─────────────────────────────────────────────────────────────── #
# Final Fixed Pipeline Function with Parameters
# Define the pipeline
@dsl.pipeline(
    name="ran_multi_prediction_pipeline_with_genai",
    description="RAN pipeline with traffic prediction, LSTM training, RandomForest, KPI anomaly detection, and GenAI forecast explainer"
)
def ran_multi_prediction_pipeline_with_genai(
    bootstrap_servers: str = "my-cluster-kafka-bootstrap.amq-streams-kafka.svc:9092",
    s3_bucket: str = "ai-cloud-ran-genai-bucket-5f0934a3-ebae-45cc-a327-e1f60d7ae15a",
    s3_key_prefix: str = "ran-pipeline",
    s3_endpoint: str = "http://s3.openshift-storage.svc:80",
    aws_access_key: str = "mo0x4vOo5DxiiiX2fqnP",
    aws_secret_key: str = "odP78ooBR0pAPaQTp6B2t+03+U0q/JPUPUqU/oZ6",
    mistral_api_key: str = "53f4a38459dabf6c3a8682888f77b714",
    mistral_api_url: str = "https://mistral-7b-instruct-v0-3-maas-apicast-production.apps.prod.rhoai.rh-aiservices-bu.com:443/v1",
    db_host: str = "mysql-service",
    db_pwd: str = "rangenai",
    db_user: str = "root",
    db_name: str = "ran_events"
    #ran-combined-metrics_output = Dataset()
):

    # Access the outputs from both components
    #ran_metrics = ran_metrics_output.outputs['ran_metrics']
    #du_metrics = du_metrics_output.outputs['du_metrics']
    #genai_forecast_explanation = dsl.Output[dsl.Dataset]()



    # 1. Stream RAN metrics (used for LSTM and traffic prediction)
    ran_metrics_output = stream_ran_metrics_to_s3_component(
        bootstrap_servers=bootstrap_servers,
        #topic_name="ran-combined-metrics",
        s3_bucket=s3_bucket,
        s3_key_prefix=s3_key_prefix,
        s3_endpoint=s3_endpoint,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        #ran_metrics=ran_metrics,  # Make sure this is an Output[Dataset] from previous components
        #du_metrics=du_metrics,
        #ran_metrics=Output[Dataset],
        #du_metrics=Output[Dataset](),
        #output_dataset=ran-combined-metrics_output,
        #max_records=500
    )

    # 2. Stream DU metrics (for future use)
    du_metrics_output = stream_du_metrics_to_s3_component(
        bootstrap_servers=bootstrap_servers,
        #topic_name="du-resource-metrics",
        s3_bucket=s3_bucket,
        s3_key_prefix=s3_key_prefix,
        s3_endpoint=s3_endpoint,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        #output_dataset=du-resource-metrics_output,
        #ran_metrics=Output[Dataset](),
        #du_metrics=Output[Dataset],
        max_records=500
    )
    
    # 4. Train RandomForest traffic prediction model
    train_traffic_predictor_model = train_traffic_predictor(
        s3_bucket=s3_bucket,
        s3_key_prefix=s3_key_prefix,
        s3_endpoint=s3_endpoint,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key
    ).after(ran_metrics_output)

    # 5. Perform anomaly detection on the data
    genai_anomaly_detection_step = genai_anomaly_detection(
        s3_bucket=s3_bucket,
        s3_key_prefix=s3_key_prefix,
        s3_endpoint=s3_endpoint,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        model_api_key=mistral_api_key,
        model_api_url=mistral_api_url,
        model_api_name="mistral-7b-instruct",
        db_host=db_host,
        db_user=db_user,
        db_pwd=db_pwd,
        db_name=db_name,
        ran_metrics_path=ran_metrics_output.outputs["ran_metrics"]
    ).after(ran_metrics_output)


# ─────────────────────────────────────────────────────────────── #
# ✅ Compile the pipeline to YAML
if __name__ == "__main__":
    Compiler().compile(
        pipeline_func=ran_multi_prediction_pipeline_with_genai,
        package_path="ran_multi_prediction_pipeline_with_genai.yaml"
    )
    print("✅ Pipeline YAML 'ran_multi_prediction_pipeline_with_genai.yaml' generated successfully.")

