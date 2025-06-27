# ====================================================================================================
# Project: RAN Anomaly Detection and Traffic Prediction using Kubeflow Pipelines and Generative AI
# Author: Dinesh Lakshmanan
# Date: June 27, 2025
# Description:
# This Kubeflow Pipeline automates an end-to-end MLOps workflow for Radio Access Network (RAN)
# performance metrics. It encompasses real-time data ingestion, persistent storage,
# machine learning model training for traffic prediction, and a robust anomaly detection
# system powered by Generative AI (GenAI) with Retrieval-Augmented Generation (RAG).
#
# The pipeline is designed to process streaming RAN data, identify key performance indicator (KPI)
# anomalies, provide intelligent explanations, and offer actionable recommendations.
#
# Pipeline Flow:
# 1.  Data Ingestion: Streams RAN metrics from Kafka and archives them to S3.
# 2.  ML Model Training: Trains RandomForest models for traffic prediction using historical data from S3.
# 3.  GenAI Anomaly Detection: Analyzes the latest RAN data for anomalies entirely using an intelligent LLM.
# 3.  GenAI Anomaly Detection: Analyzes the latest RAN data for anomalies using a multi-layered approach:
#     a.  Python-based rule engine for deterministic anomaly detection (e.g., PRB, RSRP, SINR, Throughput, UEs, Cell Outage)
#         to ensure "zero tolerance" on logical/numerical hallucinations.
#     b.  A Large Language Model (LLM) for formatting the detected anomalies into a human-readable report
#         and leveraging RAG for generating context-aware recommended fixes from documentation.
#
# Components:
# ----------------------------------------------------------------------------------------------------
# 1.  stream_ran_metrics_to_s3_component:
#     - Purpose: Ingests real-time RAN KPI data from Kafka and stores it in S3.
#     - Functionality: Connects to Kafka, consumes a batch of records, parses CSV data, handles malformed
#       records, and uploads the processed batch as a timestamped CSV file to S3.
#     - Output: An S3 URI pointing to the latest batch of raw RAN metrics.
#
# 2.  train_traffic_predictor:
#     - Purpose: Trains Machine Learning models to predict network traffic patterns.
#     - Functionality: Loads all historical RAN metric CSVs from S3, performs extensive data
#       preprocessing, feature engineering (e.g., temporal features, one-hot encoding for categorical
#       and adjacent cells), and trains a RandomForest Regressor (for UEs usage prediction)
#       and a RandomForest Classifier (for traffic class prediction).
#       Models are then saved to S3 and as pipeline artifacts.
#     - Dependencies: Runs after `stream_ran_metrics_to_s3_component` to ensure data availability.
#
# 3.  genai_anomaly_detection:
#     - Purpose: Identifies and reports anomalies in RAN KPIs using a hybrid Python-LLM approach.
#     - Functionality:
#       - **Python Logic:** Acts as the primary anomaly detection engine. It evaluates incoming RAN metrics
#         against predefined, strict numerical and logical thresholds for all specified KPIs
#         (PRB Utilization, RSRP, SINR, Throughput Drop, UEs Spike/Drop, Cell Outage).
#         It also retrieves historical data from S3 for comparative analysis rules.
#       - **LLM Logic:** Serves as the intelligent formatting and reporting layer. It receives the
#         (already verified) detected anomalies from Python and formats them into a standardized,
#         human-readable report. It uses Retrieval-Augmented Generation (RAG) with a FAISS vector
#         database to suggest specific documentation sections for recommended fixes.
#       - **Strictness Control:** Employs aggressive prompt engineering techniques (e.g., dual-path
#         prompting for 'NO_ANOMALY', explicit instructions, custom stop tokens) to minimize LLM
#         hallucinations and ensure strict adherence to output formats and brevity.
#       - **Persistence:** Logs detected anomalies to a MySQL database and saves all anomaly
#         detection results to S3.
#     - Dependencies: Runs after `stream_ran_metrics_to_s3_component` to process the latest data batch.
#
# ====================================================================================================

from kfp import dsl
from kfp.dsl import component, Input, Output, Dataset
from kfp import components
from kfp.compiler import Compiler


# Define components here (stream_ran_metrics_to_s3_component, train_traffic_predictor, genai_anomaly_detection)

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
    
    # Fill missing values
    df['Band'] = df['Band'].fillna("Unknown Band").astype(str)
    df['Area Type'] = df['Area Type'].fillna("Unknown Area").astype(str)

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

    log.info(f"Grouped data shape: {grouped.shape}")
    log.info(f"Sample grouped data:\n{grouped.head(3).to_string(index=False)}")

    grouped['Date'] = grouped['Datetime'].dt.strftime('%m-%d-%Y')
    grouped['Hour'] = grouped['Datetime'].dt.hour
    grouped['Day'] = grouped['Datetime'].dt.day
    grouped['Month'] = grouped['Datetime'].dt.month
    grouped['Weekday'] = grouped['Datetime'].dt.weekday

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
        "pyarrow",
        "fastparquet"
    ]
)
def genai_anomaly_detection(
    s3_bucket: str,
    s3_key_prefix: str,
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
    from io import StringIO
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

    # --- START OF HELPER FUNCTIONS ---

    def get_historical_data(s3_client_obj, bucket_name, key_prefix, cell_id, band, current_record_datetime, num_rows=3):
        historical_records = []
        common_search_prefix = f"{key_prefix}/ran-combined-metrics/"

        try:
            response = s3_client_obj.list_objects_v2(Bucket=bucket_name, Prefix=common_search_prefix)
            if 'Contents' not in response:
                log.info(f"No objects found under {common_search_prefix} for historical data.")
                return pd.DataFrame()

            sorted_objects = sorted(
                [obj for obj in response['Contents'] if obj['Key'].endswith('.csv')],
                key=lambda x: x['LastModified'],
                reverse=True
            )

            history_column_names = [
                "Cell ID", "Datetime", "Band", "Frequency", "UEs Usage", "Area Type", "Lat", "Lon", "City", "Adjacent Cells",
                "RSRP", "RSRQ", "SINR", "Throughput (Mbps)", "Latency (ms)", "Max Capacity"
            ]

            found_count = 0
            for obj in sorted_objects:
                if found_count >= num_rows:
                    break

                obj_key = obj['Key']
                log.debug(f"Checking historical file: {obj_key}")

                csv_obj_response = s3_client_obj.get_object(Bucket=bucket_name, Key=obj_key)
                body = csv_obj_response['Body'].read().decode('utf-8')

                temp_df = pd.read_csv(StringIO(body), names=history_column_names, header=0)

                temp_df['Datetime'] = pd.to_datetime(temp_df['Datetime'])
                
                filtered_history = temp_df[
                    (temp_df['Cell ID'] == cell_id) &
                    (temp_df['Band'] == band) &
                    (temp_df['Datetime'] < current_record_datetime)
                ].sort_values(by='Datetime', ascending=False)

                for _, row in filtered_history.iterrows():
                    if found_count < num_rows:
                        historical_records.append(row.to_dict())
                        found_count += 1
                    else:
                        break

            if historical_records:
                final_history_df = pd.DataFrame(historical_records, columns=history_column_names)
                final_history_df['Datetime'] = pd.to_datetime(final_history_df['Datetime'])
                return final_history_df.sort_values(by='Datetime', ascending=True)
            else:
                return pd.DataFrame(columns=history_column_names)

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
            max_tokens=250, # Reduced max_tokens to save context window.
            temperature=0.1, # Keep temperature low for deterministic output
            # --- ADD STOP SEQUENCE HERE ---
            stop=["\nExplanation:", "\nNote:", "\nBest regards,"] # Add common unwanted phrases
        )
        embedding = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")

        log.info("Loading FAISS index from S3")
        download_index_from_s3(s3_bucket, "faiss_index", "/tmp/faiss_index")
        vectordb = FAISS.load_local("/tmp/faiss_index", embeddings=embedding, allow_dangerous_deserialization=True)

        retriever = vectordb.as_retriever()
        qa_chain = RetrievalQA.from_chain_type(llm=llm, chain_type="stuff", retriever=retriever, return_source_documents=True)
        log.info("Chain created")
        return qa_chain

    # --- PYTHON NOW HANDLES ALL ANOMALY DETECTION LOGIC ---
    # This ensures "zero tolerance" on calculation and comparison errors.
    def check_anomalies_in_python(record_data, historical_data_df):
        anomalies_found = []
        
        def safe_float(value, metric_name):
            try:
                return float(value)
            except (ValueError, TypeError):
                log.warning(f"Invalid value for {metric_name}: '{value}'. Skipping check.")
                return None

        # Extract values (using safe_float)
        ues_usage = safe_float(record_data.get("UEs Usage"), "UEs Usage")
        max_capacity = safe_float(record_data.get("Max Capacity"), "Max Capacity")
        rsrp = safe_float(record_data.get("RSRP"), "RSRP")
        sinr = safe_float(record_data.get("SINR"), "SINR")
        throughput = safe_float(record_data.get("Throughput (Mbps)"), "Throughput (Mbps)")
        rsrq = safe_float(record_data.get("RSRQ"), "RSRQ")

        # --- Rule 1: High PRB Utilization ---
        if ues_usage is not None and max_capacity is not None:
            if max_capacity > 0:
                prb_utilization = (ues_usage / max_capacity) * 100
                if prb_utilization > 95.0: # STRICTLY GREATER THAN 95.0%
                    anomalies_found.append(f"High PRB Utilization: {prb_utilization:.2f}% > 95.0%")
            elif ues_usage > 0: # Max Capacity is 0 and UEs Usage > 0
                 anomalies_found.append(f"PRB Utilization Error: UEs Usage ({ues_usage}) with Zero Max Capacity (0).")

        # --- Rule 2: Low RSRP ---
        if rsrp is not None and rsrp < -110: # RSRP < -110 dBm
            anomalies_found.append(f"Low RSRP: {rsrp} dBm < -110 dBm")

        # --- Rule 3: Low SINR ---
        if sinr is not None and sinr < 0: # SINR < 0 dB
            anomalies_found.append(f"Low SINR: {sinr} dB < 0 dB")

        # --- Rule 4: Throughput Drop (Requires Historical Data) ---
        if len(historical_data_df) >= 3 and throughput is not None:
            try:
                prior_throughputs = historical_data_df['Throughput (Mbps)'].astype(float)
                avg_prior_throughput = prior_throughputs.mean()
                if avg_prior_throughput > 0 and throughput < 0.5 * avg_prior_throughput: # current < 0.5 * avg_prior
                    anomalies_found.append(f"Throughput Drop: {throughput:.2f} Mbps (Current) vs. {avg_prior_throughput:.2f} Mbps (Avg Prior 3) - drop > 50%")
            except (ValueError, TypeError, ZeroDivisionError) as e:
                log.warning(f"Error checking Throughput Drop for Cell ID {record_data['Cell ID']}: {e}")
        elif len(historical_data_df) < 3:
            log.debug(f"Throughput Drop rule skipped for Cell ID {record_data['Cell ID']}: Insufficient history.")

        # --- 5. UEs Spike/Drop (Requires Historical Data) ---
        if len(historical_data_df) >= 3 and ues_usage is not None:
            try:
                prior_ues = historical_data_df['UEs Usage'].astype(float)
                avg_prior_ues = prior_ues.mean()
                if avg_prior_ues > 0 and (abs(ues_usage - avg_prior_ues) / avg_prior_ues) > 0.5: # abs(change)/avg_prior > 0.5
                    anomalies_found.append(f"UEs Spike/Drop: {ues_usage} UEs (Current) vs. {avg_prior_ues:.2f} UEs (Avg Prior 3) - change > 50%")
            except (ValueError, TypeError, ZeroDivisionError) as e:
                log.warning(f"Error checking UEs Spike/Drop for Cell ID {record_data['Cell ID']}: {e}")
        elif len(historical_data_df) < 3:
            log.debug(f"UEs Spike/Drop rule skipped for Cell ID {record_data['Cell ID']}: Insufficient history.")
        
        # --- Rule 6: Cell Outage ---
        if (ues_usage is not None and ues_usage == 0 and
            throughput is not None and throughput == 0 and
            sinr is not None and sinr <= -10 and
            rsrp is not None and rsrp <= -120 and
            rsrq is not None and rsrq <= -20):
            anomalies_found.append(f"Cell Outage: UEs=0, Tput=0, SINR<=-10 ({sinr}), RSRP<=-120 ({rsrp}), RSRQ<=-20 ({rsrq})")
        
        return anomalies_found # Return list of anomaly strings

    # --- verify_anomaly_block is now obsolete/removed ---
    # Its logic has been moved and is fully integrated into `check_anomalies_in_python`.
    df_current_batch = pd.read_csv(StringIO(current_batch_csv_data))

    log.info(f"Unique bands before filtering: {df_current_batch['Band'].unique().tolist()}")
    log.info(f"Row count before filtering: {df_current_batch.shape[0]}")

    VALID_BANDS = ['Band 29', 'Band 26', 'Band 71', 'Band 66']
    df_current_batch = df_current_batch[df_current_batch['Band'].isin(VALID_BANDS)]

    log.info(f"Unique bands after filtering: {df_current_batch['Band'].unique().tolist()}")
    log.info(f"Row count after filtering: {df_current_batch.shape[0]}")

    band_map_str = df_current_batch[['Cell ID', 'Band']].drop_duplicates().head(100).to_csv(index=False) # Not explicitly used in this prompt version

    # --- SIMPLIFIED PROMPT TEMPLATE ---
    # LLM's role is now primarily formatting the output based on anomalies provided by Python,
    # and using its RAG capability for suggested fixes.
    prompt_template = """
    You are a professional Radio Access Network (RAN) anomaly reporting assistant. Your task is to accurately summarize detected anomalies and provide a standard recommended fix from Baicells documentation.

    **Current Cell Context:**
    - Cell ID: {current_cell_id}
    - Band: {current_band}
    - Data: {current_chunk}

    **Detected Anomalies (from automated system analysis - THESE ARE VERIFIED):**
    {anomalies_list_from_python}

    ---

    **Instructions:**
    1. If "Detected Anomalies" contains **"NO_ANOMALIES_DETECTED"**, respond **EXACTLY** with:
       `NO_ANOMALY`
    2. If "Detected Anomalies" lists **any specific anomalies**, respond **EXACTLY** in the following format (including tags and line breaks):

    **** START_EVENT **** ANOMALY_DETECTED  
    1. Cell ID {current_cell_id}, {current_band}  
    - <A concise, bulleted list of ALL metrics that violated thresholds, exactly as provided in "Detected Anomalies">
    - Explanation: <Provide a single, precise, and crisp explanation for the detected anomaly(ies). Use your knowledge base to explain the common implications or root causes of these specific anomalies. Do not include introductory phrases like "The anomaly is...". Just the explanation.>  
    - Recommended fix: <Use your knowledge base (Baicells documentation) to suggest a specific section for the most relevant anomaly listed. If multiple, pick the primary one. If no specific section, use "Refer to Baicells documentation (Section X.X, Page Y)">  
    **** END_EVENT ****

    ---

    **Important Rules:**
    - DO NOT add any anomalies not explicitly provided in "Detected Anomalies".
    - DO NOT remove any anomalies explicitly provided in "Detected Anomalies".
    - DO NOT include explanations, calculations, or additional text outside the specified output format.
    - DO NOT include any introductory or concluding remarks.
    - Be concise in the bulleted list.

    """

    qa_chain = get_chain()
    log.info("Execute anomaly detection")

    anomaly_results_df = pd.DataFrame(columns=[
        "Cell ID", "Band", "Datetime", "LLM_Raw_Response", "Is_Anomaly_Final", "Final_Anomaly_Reason"
    ])

    df_current_batch_for_records = df_current_batch.to_dict(orient='records')
    validated_anomaly_blocks_for_db = []

    for record_dict in df_current_batch_for_records:
        current_cell_id = record_dict["Cell ID"]
        current_band = record_dict["Band"]
        current_datetime = datetime.strptime(record_dict["Datetime"], "%Y-%m-%d %H:%M:%S")

        # --- STEP 1: Detect Anomalies using Python Code (THE SOURCE OF TRUTH) ---
        history_df_for_detection = get_historical_data(s3, s3_bucket, s3_key_prefix, current_cell_id, current_band, current_datetime, num_rows=3)
        detected_anomalies_by_python = check_anomalies_in_python(record_dict, history_df_for_detection)

        # Prepare anomalies list for LLM prompt
        anomalies_for_prompt = ""
        if detected_anomalies_by_python:
            anomalies_for_prompt = "\n".join([f"- {a}" for a in detected_anomalies_by_python])
            log.info(f"Python detected anomalies for Cell ID {current_cell_id}, {current_band}: {anomalies_for_prompt}")
        else:
            anomalies_for_prompt = "NO_ANOMALIES_DETECTED"
            log.info(f"Python detected no anomalies for Cell ID {current_cell_id}, {current_band}.")

        # --- STEP 2: Use LLM for Formatting and Reporting (and RAG for fix) ---
        # Format the current record as a single-row CSV for the LLM
        current_chunk_io = StringIO()
        pd.DataFrame([record_dict]).to_csv(current_chunk_io, index=False, header=False, quoting=csv.QUOTE_ALL)
        current_chunk_str = current_chunk_io.getvalue().strip()
        
        # Prepare the query for documentation retrieval within the prompt
        doc_query = ""
        if detected_anomalies_by_python:
            # Ask LLM to get a fix for the *first* detected anomaly as an example
            doc_query = f"Recommended fix for: {detected_anomalies_by_python[0]}"
        else:
            doc_query = "General Baicells documentation" # Placeholder for no anomaly

        # The qa_chain.invoke normally takes a dictionary with 'query' key for RAG.
        # Here we are passing the full prompt directly to the LLM.
        # To make LLM use RAG for fix, we need to adapt this part.
        # Option A: Ask LLM to generate the fix based on its knowledge/RAG implicitly (less controlled).
        # Option B: Use qa_chain.invoke for the RAG part separately.

        # Let's use Option A by including the instruction in the prompt.
        # The prompt already has "Use your knowledge base (Baicells documentation) to suggest a specific section..."
        # This implies the RAG part of qa_chain is providing context to the LLM to answer the prompt.
        
        prompt_with_rag_context = prompt_template.format(
            current_cell_id=current_cell_id,
            current_band=current_band,
            current_chunk=current_chunk_str,
            anomalies_list_from_python=anomalies_for_prompt,
            # We don't need a separate placeholder for doc_query, it's implicit in instruction
        )

        log.info(f"Running LLM for Cell ID: {current_cell_id}, Band: {current_band} (for formatting and fix)")
        log.debug(f"Full Prompt for {current_cell_id}, {current_band}:\n{prompt_with_rag_context}")
        
        llm_raw_response = "LLM_CALL_FAILED"
        try:
            # We are calling qa_chain.invoke(prompt) directly, which implies prompt has everything.
            # The 'retriever' part of qa_chain should add relevant documents to the context based on prompt.
            # The prompt text itself is the 'query' for the retriever here.
            response = qa_chain.invoke(prompt_with_rag_context) 
            llm_raw_response = response['result']
        except Exception as llm_e:
            log.error(f"LLM API call failed for Cell ID {current_cell_id}, Band {current_band}: {llm_e}", exc_info=True)

        log.info(f"LLM Raw Response for Cell ID {current_cell_id}, Band {current_band}:\n{llm_raw_response}\n")

        # --- STEP 3: Finalize Results Based on Python's Truth ---
        # This parsing now primarily checks if LLM adhered to formatting.
        # The Is_Anomaly_Final is based on Python's decision.
        final_is_anomaly = False
        final_reason = "N/A"
        
        match = re.search(r"\*\*\*\* START_EVENT \*\*\*\*(.*?)\*\*\*\* END_EVENT \*\*\*\*", llm_raw_response, re.DOTALL)
        
        if detected_anomalies_by_python: # If Python found anomalies, we expect LLM to format it.
            final_is_anomaly = True
            if match and "ANOMALY_DETECTED" in match.group(1):
                final_reason = match.group(1).strip() # Use LLM's formatted output as reason
                log.info(f"LLM successfully formatted anomaly for Cell ID {current_cell_id}, Band {current_band}.")
                # Add to DB list (contains LLM's formatted text, which summarizes Python's findings)
                validated_anomaly_blocks_for_db.append(f"Cell ID {current_cell_id}, Band {current_band}:\n{final_reason}")
            else:
                final_reason = f"LLM_FORMAT_ERROR: Python detected anomalies: {'; '.join(detected_anomalies_by_python)}. LLM failed to format: {llm_raw_response}"
                log.error(final_reason)
                # Still add to DB list if formatting error is acceptable, or skip if strict.
                # For "zero tolerance" on detection, Python is the source. If LLM messes up format, we record error.
                validated_anomaly_blocks_for_db.append(f"Cell ID {current_cell_id}, Band {current_band} (FORMAT ERROR):\n{final_reason}")

        else: # If Python found NO anomalies, we expect LLM to say NO_ANOMALY.
            final_is_anomaly = False
            if llm_raw_response.strip() == "NO_ANOMALY":
                final_reason = "NO_ANOMALY"
                log.info(f"LLM correctly reported NO_ANOMALY for Cell ID {current_cell_id}, Band {current_band}.")
            else:
                final_reason = f"LLM_FORMAT_ERROR: Python found no anomalies, but LLM responded: {llm_raw_response}"
                log.warning(final_reason)
        
        # Append result to DataFrame
        new_row_df = pd.DataFrame([{
            "Cell ID": current_cell_id,
            "Band": current_band,
            "Datetime": record_dict["Datetime"],
            "LLM_Raw_Response": llm_raw_response, # Store the full raw response from LLM
            "Is_Anomaly_Final": final_is_anomaly, # Based on Python's decision
            "Final_Anomaly_Reason": final_reason # LLM's formatted reason or error message
        }])
        anomaly_results_df = pd.concat([anomaly_results_df, new_row_df], ignore_index=True)

    # --- FINAL DATABASE INSERTION ---
    if validated_anomaly_blocks_for_db:
        combined_anomalies_for_db = "\n\n".join(validated_anomaly_blocks_for_db)
        log.info("Total validated anomalies to insert into DB:\n%s", combined_anomalies_for_db)

        full_current_batch_csv = df_current_batch.to_csv(index=False)

        insert_event_db(combined_anomalies_for_db, "auto_detected_anomaly", full_current_batch_csv)
    else:
        log.info("No validated anomalies detected in any chunk for this run. Not inserting into DB.")

    # Final output of the component: Save the anomaly detection results DataFrame to S3
    anomaly_output_key = f"anomaly_results/{timestamp_str}_anomalies.csv"
    anomaly_output_uri = f"s3://{s3_bucket}/{anomaly_output_key}"

    anomaly_output_buffer = StringIO()
    anomaly_results_df.to_csv(anomaly_output_buffer, index=False, quoting=csv.QUOTE_ALL)
    s3.put_object(Bucket=s3_bucket, Key=anomaly_output_key, Body=anomaly_output_buffer.getvalue())

    log.info(f"\nLLM anomaly analysis results saved to {anomaly_output_uri}")

    log.info("Anomaly detection component finished.")


# ─────────────────────────────────────────────────────────────── #
# Final Fixed Pipeline Function with Parameters
# Define the pipeline
@dsl.pipeline(
    name="ran_multi_prediction_pipeline_with_genai",
    description="RAN pipeline with stream RAN metrics to S3, train traffic precition model and GenAI anomaly detection"
)
def ran_multi_prediction_pipeline_with_genai(
    bootstrap_servers: str = "my-cluster-kafka-bootstrap.amq-streams-kafka.svc:9092",
    s3_bucket: str = "ai-cloud-ran-genai-bucket-5f0934a3-ebae-45cc-a327-e1f60d7ae15a",
    s3_key_prefix: str = "ran-pipeline",
    s3_endpoint: str = "http://s3.openshift-storage.svc:80",
    aws_access_key: str = "mo0x4vOo5DxiiiX2fqnP",
    aws_secret_key: str = "odP78ooBR0pAPaQTp6B2t+03+U0q/JPUPUqU/oZ6",
    #llm_api_key: str = "53f4a38459dabf6c3a8682888f77b714",
    llm_api_key: str = "08e38386e70547b185b8894e13524db5",
    #llm_api_url: str = "https://mistral-7b-instruct-v0-3-maas-apicast-production.apps.prod.rhoai.rh-aiservices-bu.com:443/v1",
    llm_api_url: str = "https://llama-3-1-8b-instruct-maas-apicast-production.apps.prod.rhoai.rh-aiservices-bu.com:443/v1",
    db_host: str = "mysql-service",
    db_pwd: str = "rangenai",
    db_user: str = "root",
    db_name: str = "ran_events"
    #ran-combined-metrics_output = Dataset()
):

    # 1. Stream RAN metrics (used for LSTM and traffic prediction)
    ran_metrics_output = stream_ran_metrics_to_s3_component(
        bootstrap_servers=bootstrap_servers,
        #topic_name="ran-combined-metrics",
        s3_bucket=s3_bucket,
        s3_key_prefix=s3_key_prefix,
        s3_endpoint=s3_endpoint,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
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
        model_api_key=llm_api_key,
        model_api_url=llm_api_url,
        #model_api_name="mistral-7b-instruct",
        model_api_name="meta-llama/Llama-3.1-8B-Instruct",
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

