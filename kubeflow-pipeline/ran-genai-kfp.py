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
            max_poll_records=1, # <<< Set this to 1 because each Kafka message is one large CSV block
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
    packages_to_install=["openai", "langchain", "sqlalchemy", "langchain-community", "sentence-transformers", "faiss-cpu", "pymysql","boto3", "pandas", "requests",
    "click>=8.0.0,<9",
    "docstring-parser>=0.7.3,<1",
    "google-api-core!=2.0.*,!=2.1.*,!=2.2.*,!=2.3.0,<3.0.0dev,>=1.31.5",
    "google-auth>=1.6.1,<3",
    "google-cloud-storage>=2.2.1,<4",
    #"kfp-pipeline-spec==0.6.0",
    #"kfp-server-api>=2.1.0,<2.5.0",
    "kubernetes>=8.0.0,<31",
    "protobuf>=4.21.1,<5",
    "tabulate>=0.8.6,<1"]
)
def genai_anomaly_detection(
    s3_bucket: str,
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
    ran_metrics_path: Input[Dataset]
    #metrics_output: Output[Dataset]
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
    from datetime import datetime

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

    # Read CSV contents as string from ran_metrics_path artifact
    with open(ran_metrics_path.path, "r") as f:
        csv_data = f.read()
   
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

    '''
    # Load the CSV artifact dynamically
    ran_df = pd.read_csv(ran_metrics_path.path)

    # Optional: Downsample or limit rows for token size constraints
    sampled_df = ran_df.sample(n=20, random_state=42) if len(ran_df) > 20 else ran_df
    '''
    
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
            #model_name="deepseek-r1-distill-qwen-14b",
            model_name=model_api_name,
            #model_name="r1-qwen-14b-w4a16",
            #model_name="meta-llama/Llama-3.1-8B-Instruct",
            max_tokens=2000,
            #model_kwargs={"stop": ["."]},
            temperature=0.7,
            #http_client=custom_http_client
        )
        embedding = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")

        log.info("Loading FAISS index from S3")
        download_index_from_s3(s3_bucket, "faiss_index", "/tmp/faiss_index")
        vectordb = FAISS.load_local("/tmp/faiss_index", embeddings=embedding, allow_dangerous_deserialization=True)

        retriever = vectordb.as_retriever()
        qa_chain = RetrievalQA.from_chain_type(llm=llm, chain_type="stuff", retriever=retriever, return_source_documents=True)
        log.info("Chain created")
        return qa_chain

    # Convert CSV string to DataFrame
    from io import StringIO
    df = pd.read_csv(StringIO(csv_data))

    # Log band distribution before filtering
    log.info(f"Unique bands before filtering: {df['Band'].unique().tolist()}")
    log.info(f"Row count before filtering: {df.shape[0]}")

    # Filter by valid bands
    VALID_BANDS = ['Band 29', 'Band 26', 'Band 71', 'Band 66']
    df = df[df['Band'].isin(VALID_BANDS)]

    # Log band distribution after filtering
    log.info(f"Unique bands after filtering: {df['Band'].unique().tolist()}")
    log.info(f"Row count after filtering: {df.shape[0]}")

    '''
    # Convert filtered DataFrame back to CSV string
    csv_data = df.to_csv(index=False)    

    # Optional: truncate to avoid token overflow (adjust as needed)
    csv_data = csv_data[:3000] if len(csv_data) > 3000 else csv_data   

    # Get the actual Band-Cell mapping in CSV preview
    band_map_str = df[['Cell ID', 'Band']].drop_duplicates().to_csv(index=False) 
    '''

    #band_map_str = df[['Cell ID', 'Band']].drop_duplicates().to_csv(index=False)
    band_map_str = df[['Cell ID', 'Band']].drop_duplicates().head(100).to_csv(index=False)



    # STEP 1: Chunking with token control
    #from langchain.text_splitter import RecursiveCharacterTextSplitter

    def chunk_dataframe_by_token_limit(df, max_chars=2500):
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=max_chars, chunk_overlap=0)
        chunks = []

        grouped = df.groupby(["Cell ID", "Band"])  # Group by both Cell ID and Band
        for (cell_id, band), group in grouped:
            csv_text = group.to_csv(index=False)
            split_chunks = text_splitter.split_text(csv_text)
            for subchunk in split_chunks:
                chunks.append({
                    "cell_id": cell_id,
                    "band": band,
                    "text": subchunk
                })
        return chunks

    # Apply chunking with token-safe limits
    chunked_cells = chunk_dataframe_by_token_limit(df)
    log.info(f"Total token-safe chunks to process: {len(chunked_cells)}")

    # STEP 2: Define prompt template
    prompt_template = """
    You are a Radio Access Network (RAN) engineer and telecom operations expert with access to Baicells BaiBNQ gNodeB technical documentation.

    Each row in the dataset includes the following columns:

    - Timestamp: Time of metric capture  
    - Cell ID: Unique identifier of the cell site  
    - Band: Frequency band used (e.g., 71, 66, 26, 29)  
    - SINR (dB): Signal to Interference plus Noise Ratio  
    - RSRP (dBm): Reference Signal Received Power  
    - RSRQ (dB): Reference Signal Received Quality  
    - UEs Usage: Number of connected user equipment (UEs)  
    - Max Capacity: Maximum UE capacity for the cell  

    Your task is to analyze the provided 5G RAN performance data **strictly using the anomaly thresholds below**.

    ---

    ### RULE CHECKLIST (Apply each to this row only)

    1. **High PRB Utilization**  
    - Formula: PRB Utilization = (UEs Usage / Max Capacity) × 100  
    - Threshold: Anomaly if **PRB Utilization > 95.0%**
    - Example:  
        - UEs = 51, Max Capacity = 62  
        - PRB Utilization = (51 / 62) × 100 = 82.26% -> Not an anomaly  
        - Do not report unless PRB Utilization > 95.0%

    2. **Low RSRP**: Anomaly if RSRP < -110 dBm  
    3. **Low SINR**: Anomaly if SINR < 0 dB  
    4. **Throughput drop**: More than 50% drop vs. average of 3 prior rows (same Cell ID + Band). Ignore if no history.  
    5. **UEs spike/drop**: >50% change vs. 3 prior rows (same Cell ID + Band). Ignore if no history.  
    6. **Cell outage**: Must meet **ALL** conditions:  
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

    ---

    ### ANALYSIS INSTRUCTIONS

    - Only analyze the following row with **Cell ID: {cell_id}** and **Band: {band}**
    - Evaluate each rule in the checklist above **strictly**
    - For PRB Utilization, show the full calculation and threshold comparison step

    ---

    ### FORMAT REQUIREMENTS

    If **any** anomaly is found, respond exactly like this:

    **** START_EVENT ****  
    ANOMALY_DETECTED  
    1. Cell ID X, Band Y  
    - <Metric and reason that violated the threshold>  
    - Recommended fix: Refer to Baicells documentation (Section X.X, Page Y)  
    **** END_EVENT ****

    If the row is **not** anomalous, respond with:

    **NO_ANOMALY**

    ---

    Use ONLY valid Band values from the list below:  
    {band_map}

    Below is a dataset of RAN metrics including Cell ID and Band. Analyze this row independently:

    DATA:  
    {chunk}
    """.strip()


    qa_chain = get_chain()
    log.info("Execute anomaly detection")
    #response = qa_chain.invoke(prompt)

    # STEP 3: Replace cell loop with token-safe version
    results = []

    for chunk in chunked_cells:  # chunked_cells was created in Step 1
        chunk_text = chunk['text']
        cell_id = chunk['cell_id']
        band = chunk['band']

        prompt = prompt_template.format(
            band_map=band_map_str,
            chunk=chunk_text,
            cell_id=cell_id,
            band=band
        )

        #log.info(f"Running LLM for Cell ID: {cell_id}, Band: {band}")
        log.info(f"Running LLM for Cell ID: {cell_id}, {band}")
        response = qa_chain.invoke(prompt)
        result_text = response['result']
        #results.append(result_text)
        results.append({
            "cell_id": cell_id,
            "band": band,
            "response": result_text
        })

        print(result_text)
        # NEW: Log high-level status per cell and band
        if 'ANOMALY_DETECTED' in result_text:
            #log.info(f"Anomaly detected for Cell ID {cell_id}, Band {band}")
            log.info(f"Anomaly detected for Cell ID {cell_id}, {band}")
        else:
            #log.info(f"No anomaly found for Cell ID {cell_id}, Band {band}")
            log.info(f"No anomaly found for Cell ID {cell_id}, {band}")


    anomaly_blocks = []


    def verify_anomaly_block(text_block):
        """
        Verify that the values in the anomaly report actually violate thresholds.
        If they do not, this is likely a hallucination.
        """
        sinr = re.search(r"SINR.*?(-?\d+\.?\d*)\s*dB", text_block)
        rsrp = re.search(r"RSRP.*?(-?\d+\.?\d*)\s*dBm", text_block)
        rsrq = re.search(r"RSRQ.*?(-?\d+\.?\d*)\s*dB", text_block)
        prb = re.search(r"PRB.*?(\d+\.?\d*)\s*%", text_block)
        ue = re.search(r"UE.*?(\d+)", text_block)

        # Parse floats
        sinr_val = float(sinr.group(1)) if sinr else None
        rsrp_val = float(rsrp.group(1)) if rsrp else None
        rsrq_val = float(rsrq.group(1)) if rsrq else None
        prb_val = float(prb.group(1)) if prb else None
        ue_val = int(ue.group(1)) if ue else None

        log.debug(f"Parsed values - SINR: {sinr_val}, RSRP: {rsrp_val}, RSRQ: {rsrq_val}, PRB: {prb_val}, UE: {ue_val}")

        # Thresholds
        if (
            (sinr_val is not None and sinr_val < -10) or
            (rsrp_val is not None and rsrp_val < -110) or
            (rsrq_val is not None and rsrq_val < -15) or
            (prb_val is not None and prb_val > 95) or
            (ue_val is not None and ue_val > 20)
        ):
            return True  # Valid anomaly
        return False  # Likely hallucinated

    for idx, item in enumerate(results):
        cell_id = item.get("cell_id", f"cell_{idx}")
        band = item.get("band", "unknown")
        result_text = item.get("response", "")
        log.info(f"Response for Cell ID {cell_id}, {band}: {result_text}")
        #log.info(f"Response for Cell ID {cell_id}: {result_text}")

        match = re.search(r"\*\*\*\* START_EVENT \*\*\*\*(.*?)\*\*\*\* END_EVENT \*\*\*\*", result_text, re.DOTALL)
        if match:
            anomaly_text = match.group(1).strip()

            if verify_anomaly_block(anomaly_text):
                anomaly_blocks.append(f"Cell ID {cell_id}, {band}:\n{anomaly_text}")
                log.info(f"Anomaly validated for Cell ID {cell_id}, {band}")
            else:
                log.warning(f"Anomaly block failed validation for Cell ID {cell_id}, {band}:\n{anomaly_text}")
        else:
            log.info(f"No structured anomaly found for Cell ID {cell_id}, {band}")


    #  If any anomaly blocks were found, log and save to DB
    if anomaly_blocks:
        combined_anomalies = "\n\n".join(anomaly_blocks)
        log.info("Detected anomalies:\n%s", combined_anomalies)

        # Save full DataFrame as CSV string
        full_csv = df.to_csv(index=False)

        # Insert to DB: (event_text, type, data)
        insert_event_db(combined_anomalies, "auto_detected_anomaly", full_csv)
    else:
        log.info("No anomalies detected in any chunk.")



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

