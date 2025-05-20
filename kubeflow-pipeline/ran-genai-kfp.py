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
    topic_name = "ran-combined-metrics"

    column_names = [
        "Cell ID", "Datetime", "Band", "Frequency", "UEs Usage", "Area Type", "Adjacent Cells", 
        "RSRP", "RSRQ", "SINR", "Throughput (Mbps)", "Latency (ms)", "Max Capacity"
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
    #csv_data = "\n".join(collected_lines)
    #df = pd.read_csv(StringIO(csv_data), header=None)


    records = []
    for line in collected_lines:
        try:
            parsed = next(csv.reader([line]))
            if len(parsed) == len(column_names):
                records.append(parsed)
            else:
                print(f"Skipping malformed record: {line}")
        except Exception as e:
            print(f"CSV parsing failed: {e} | Content: {line}")

    '''
    records = []
    for line in collected_lines:
        try:
            values = line.strip().split(',')
            # Always 13 final fields expected: total columns = 13
            if len(values) < 13:
                print(f"Skipping short record: {line}")
                continue

            # Reverse split: take last 7 fixed fields
            fixed_tail = values[-7:]
            # Extract first fixed fields
            cell_id, timestamp, band, frequency, area_type = values[:5]
            # Remaining are adjacent cells
            adjacent_cells = ",".join(values[5:-7])

            final_values = [cell_id, timestamp, band, frequency, area_type, adjacent_cells] + fixed_tail

            if len(final_values) == len(column_names):
                records.append(final_values)
            else:
                print(f"Bad record (wrong final size): {line}")
        except Exception as e:
            print(f"Failed parsing line: {e}, content: {line}")
    '''


    df = pd.DataFrame(records, columns=column_names)



    # Upload to S3
    #print(f"\n Uploading to S3")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_object_key = f"{s3_key_prefix}/{topic_name}_{timestamp}.csv"

    # Clean Adjacent Cells column
    if 'Adjacent Cells' in df.columns:
        df['Adjacent Cells'] = df['Adjacent Cells'].apply(
            lambda x: ','.join(map(str, x)) if isinstance(x, list) else str(x)
        )

    csv_buffer = StringIO()
    #df.to_csv(csv_buffer, index=False, header=False)
    df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_ALL)

    # Now get the contents with quotes around every field
    csv_string = csv_buffer.getvalue()
    print(csv_string)  # Should show all fields quoted


    s3_client.put_object(Bucket=s3_bucket, Key=s3_object_key, Body=csv_buffer.getvalue())
    #s3_client.put_object(Bucket=s3_bucket, Key=s3_object_key)
    s3_uri = f"s3://{s3_bucket}/{s3_object_key}"
    print(f"\n Data successfully uploaded to {s3_uri}")

    # Output to kubeflow artifacts
    with open(ran_metrics.path, 'w') as f:
        df.to_csv(f, index=False, header=True)

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
    s3_object_key = f"{s3_key_prefix}/{topic_name}_{timestamp}.csv"
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
    feature_cols = ['Cell ID', 'Datetime_ts', 'Datetime', 'Frequency', 'Hour', 'Weekend', 'Day', 'Month', 'Weekday', 'UEs Usage'] + \
                [col for col in grouped.columns if col.startswith('DayOfWeek_')] + \
                [col for col in grouped.columns if col.startswith('adj_cell_')]
    X = grouped[feature_cols]
    X = pd.get_dummies(X, columns=['Frequency'], prefix='Freq')

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
    regressor_model_path = "/tmp/traffic_regressor_model_{timestamp_str}.joblib"
    classifier_model_path = "/tmp/traffic_classifier_model_{timestamp_str}.joblib"
    joblib.dump(regressor, regressor_model_path)
    joblib.dump(classifier, classifier_model_path)

    log.info("Uploading models to S3...")
    with open(regressor_model_path, 'rb') as r_file:
        s3.upload_fileobj(r_file, s3_bucket, f'{s3_key_prefix}/models/traffic_regressor_model_{timestamp_str}.joblib')
    with open(classifier_model_path, 'rb') as c_file:
        s3.upload_fileobj(c_file, s3_bucket, f'{s3_key_prefix}/models/traffic_classifier_model_{timestamp_str}.joblib')

    log.info(f"Uploaded regressor model: {s3_key_prefix}/models/traffic_regressor_model_{timestamp_str}.joblib")
    log.info(f"Uploaded classifier model: {s3_key_prefix}/models/traffic_classifier_model_{timestamp_str}.joblib")

    # Output regressor to pipeline output
    with open(regressor_model_path, 'rb') as f:
        with open(output_traffic_regressor_model.path, 'wb') as out_f:
            out_f.write(f.read())

    with open(classifier_model_path, 'rb') as f:
        with open(output_traffic_classifier_model.path, 'wb') as out_f:
            out_f.write(f.read())

    log.info("Traffic predictor models saved and uploaded successfully.")



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
    mistral_api_url: str = "https://mistral-7b-instruct-v0-3-maas-apicast-production.apps.prod.rhoai.rh-aiservices-bu.com:443/v1"
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
        max_records=500
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

    '''
    # 3. Train LSTM model on ran-combined-metrics (for KPI anomaly prediction)
    train_lstm_model = train_lstm_kpi_model(
        s3_bucket=s3_bucket,
        s3_key_prefix=s3_key_prefix,
        s3_endpoint=s3_endpoint,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key
    ).after(ran_metrics_output)
    '''

    # 4. Train RandomForest traffic prediction model
    train_traffic_predictor_model = train_traffic_predictor(
        s3_bucket=s3_bucket,
        s3_key_prefix=s3_key_prefix,
        s3_endpoint=s3_endpoint,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key
    ).after(ran_metrics_output)

    '''
    # 5. Predict traffic levels using the trained RandomForest model
    predict_traffic_levels_output = predict_traffic_levels_component(
        s3_bucket=s3_bucket,
        s3_key_prefix=s3_key_prefix,
        s3_endpoint=s3_endpoint,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        #input_data=train_traffic_predictor_model.outputs['output_model']
    ).after(train_traffic_predictor_model)

    # 6. Predict KPI anomalies using the trained LSTM model
    predict_kpi_anomalies_output = predict_kpi_anomalies_component(
        s3_bucket=s3_bucket,
        s3_key_prefix=s3_key_prefix,
        s3_endpoint=s3_endpoint,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key
    ).after(train_lstm_model)
    '''

    '''
    # 7. Generate GenAI-based insights for traffic predictions and KPI anomalies
    genai_kpi_traffic_forecast_explainer = genai_kpi_traffic_forecast_explainer_component(
        #output_kpi_anomaly_predictions=predict_kpi_anomalies_output.outputs['output_kpi_anomaly_predictions'],
        #output_traffic_predictions=predict_traffic_levels_output.outputs['output_traffic_predictions'],
        du_metrics=du_metrics_output.outputs['du_metrics'],
        mistral_api_url=mistral_api_url,
        mistral_api_key=mistral_api_key
    ).after(du_metrics_output)
    '''

    #genai_forecast_explanation = dsl.Output[dsl.Dataset]()

    '''
    # 7. Generate GenAI-based insights for traffic predictions and KPI anomalies
    genai_kpi_traffic_forecast_explainer = genai_kpi_traffic_forecast_explainer_component(
        output_kpi_anomaly_predictions=predict_kpi_anomalies_output.outputs['output_kpi_anomaly_predictions'],
        output_traffic_predictions=predict_traffic_levels_output.outputs['output_traffic_predictions'],
        du_metrics=du_metrics_output.outputs['du_metrics'],
        mistral_api_url=mistral_api_url,
        mistral_api_key=mistral_api_key
    ).after(predict_kpi_anomalies_output, predict_traffic_levels_output, du_metrics_output)

    #genai_kpi_traffic_forecast_explainer.outputs["output_explanation"]
    '''

    ''' 
    # 7. Generate GenAI-based insights for traffic predictions and KPI anomalies
    genai_kpi_traffic_forecast_explainer = genai_kpi_traffic_forecast_explainer_component(
        output_kpi_anomaly_predictions=predict_kpi_anomalies.output,
        output_traffic_predictions=predict_traffic_levels.output,
        #du_metrics=du_metrics_output.output,
        #ran_metrics=ran_metrics,
        du_metrics=du_metrics_output.outputs["du_metrics"],
        #genai_forecast_explanation=genai_forecast_explanation,
        #genai_forecast_explanation=f"{s3_key_prefix}/genai/genai_forecast_explanation.csv",  # Updated to use s3_key_prefix
        mistral_api_url=mistral_api_url,
        mistral_api_key=mistral_api_key
    ).after(predict_kpi_anomalies, predict_traffic_levels, du_metrics_output)
    '''

    '''
    # 7. Generate GenAI-based insights for traffic predictions and KPI anomalies
    genai_kpi_traffic_forecast_explainer = genai_kpi_traffic_forecast_explainer_component(
        s3_bucket=s3_bucket,
        s3_key_prefix=s3_key_prefix,
        s3_endpoint=s3_endpoint,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        deepseek_api_key=deepseek_api_key,
        deepseek_api_url=deepseek_api_url
    ).after(predict_kpi_anomalies)
    '''

# ─────────────────────────────────────────────────────────────── #
# ✅ Compile the pipeline to YAML
if __name__ == "__main__":
    Compiler().compile(
        pipeline_func=ran_multi_prediction_pipeline_with_genai,
        package_path="ran_multi_prediction_pipeline_with_genai.yaml"
    )
    print("✅ Pipeline YAML 'ran_multi_prediction_pipeline_with_genai.yaml' generated successfully.")

