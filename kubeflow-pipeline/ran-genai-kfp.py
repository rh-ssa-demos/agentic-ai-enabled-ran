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
        "Cell ID", "Datetime", "Band", "Frequency", "Area Type", "Adjacent Cells", 
        "RSRP", "RSRQ", "SINR", "Throughput (Mbps)", "Latency (ms)", "Max Capacity", "UEs Usage"
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
    #output_traffic_model: Output[Dataset]
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
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    from sklearn.model_selection import train_test_split

    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] - %(message)s')
    log = logging.getLogger()

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")


    os.environ.update({
        'AWS_ACCESS_KEY_ID': aws_access_key,
        'AWS_SECRET_ACCESS_KEY': aws_secret_key,
    })

    s3 = boto3.client('s3', endpoint_url=s3_endpoint)
    files = sorted([o['Key'] for o in s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key_prefix).get('Contents', []) if "ran-combined-metrics" in o['Key']])
    if not files:
        raise FileNotFoundError("No RAN metrics files found in S3 bucket.")
    latest_file = files[-1]
    log.info(f"Reading latest RAN metrics file: {latest_file}")

    column_names = [
        "Cell ID", "Datetime", "Band", "Frequency", "Area Type", "Adjacent Cells", 
        "RSRP", "RSRQ", "SINR", "Throughput (Mbps)", "Latency (ms)", "Max Capacity", "UEs Usage"
    ]

    raw_df = pd.read_csv(
        io.BytesIO(s3.get_object(Bucket=s3_bucket, Key=latest_file)['Body'].read())
    )

    # Check if first row is a duplicate of column names (header row repeated)
    if list(raw_df.iloc[0]) == list(raw_df.columns):
        raw_df = raw_df.iloc[1:]

    # Rename columns to expected names
    raw_df.columns = column_names
    df = raw_df

    # Ensure Cell ID is numeric and remove header-like rows
    df = df[pd.to_numeric(df['Cell ID'], errors='coerce').notnull()]
    df['Cell ID'] = df['Cell ID'].astype(int)
    #df['Max Capacity'] = df['Max Capacity'].round().astype(int)
    df['Max Capacity'] = pd.to_numeric(df['Max Capacity'], errors='coerce').round().astype('Int64')

    log.info("Rows with string Cell ID:\n" + str(df[df['Cell ID'] == 'Cell ID']))

    log.info(f"Original DataFrame shape: {df.shape}")
    log.info("Initial data sample:\n" + str(df.head()))

    #df.drop_duplicates(subset=["Cell ID", "Datetime"], inplace=True)
    log.info(f"Shape after dropping duplicate Cell ID entries: {df.shape}")

    # Parse and clean
    log.info("Converting 'Datetime' to datetime format")
    df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce')
    log.info(f"Datetime conversion - non-null count: {df['Datetime'].notnull().sum()} / {len(df)}")
    df = df.dropna(subset=['Datetime'])

    log.info("Handling 'Frequency' column")
    def handle_frequency(freq):
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

    log.info(f"Frequency sample after processing:\n{df['Frequency'].head()}")
    df['Frequency'] = df['Frequency'].apply(handle_frequency).fillna(0)
   
    log.info("Parsing numeric columns")
    df['Adjacent Cells'] = pd.to_numeric(df['Adjacent Cells'], errors='coerce').fillna(0)
    df['UEs Usage'] = pd.to_numeric(df['UEs Usage'], errors='coerce').fillna(0)

    log.info("Filling Band and Area Type")
    df['Band'] = df['Band'].fillna("Unknown Band").astype(str)
    df['Area Type'] = df['Area Type'].fillna("Unknown Area").astype(str)

    # Add datetime derived features
    log.info("Deriving datetime-based features")
    df['DayOfWeek'] = df['Datetime'].dt.day_name()
    df['Hour'] = df['Datetime'].dt.hour
    df['Weekend'] = df['DayOfWeek'].isin(['Saturday', 'Sunday']).astype(int)
    log.info(f"DayOfWeek and Hour sample:\n{df[['DayOfWeek', 'Hour', 'Weekend']].head()}")

    log.info(f"Row count before grouping: {df.shape[0]}")


    # Group and aggregate
    log.info("Grouping by 'Cell ID' and 'Datetime'")
    grouped = df.groupby(['Cell ID', 'Datetime']).agg({
        'UEs Usage': 'sum',
        'Frequency': lambda x: ','.join(map(str, x.unique())),
        'DayOfWeek': 'first',
        'Hour': 'first',
        'Weekend': 'first'
    }).reset_index()
    log.info(f"Grouped sample:\n{grouped.head()}")

    log.info(f"Row count after grouping: {grouped.shape[0]}")


    # Define traffic class
    log.info("Classifying traffic based on usage")
    grouped['Traffic Class'] = np.where(grouped['UEs Usage'] >= 40, 1, 0)
    log.info(f"Traffic Class distribution:\n{grouped['Traffic Class'].value_counts()}")

    # Prepare features
    log.info("Preparing one-hot encoded features")
    grouped['Day'] = grouped['Datetime'].dt.day
    grouped['Month'] = grouped['Datetime'].dt.month
    grouped['Weekday'] = grouped['Datetime'].dt.weekday
    #features = grouped[['Cell ID', 'DayOfWeek', 'Hour', 'Weekend']]
    #features = grouped[['Cell ID', 'Datetime', 'DayOfWeek', 'Hour', 'Weekend']]
    features = grouped[['Cell ID', 'DayOfWeek', 'Hour', 'Weekend', 'Day', 'Month', 'Weekday']]
    features = pd.get_dummies(features)
    log.info(f"Feature sample after encoding:\n{features.head()}")

    target_usage = grouped['UEs Usage']
    target_class = grouped['Traffic Class']

    # Split
    X_train, X_test, y_train_usage, y_test_usage, y_train_class, y_test_class = train_test_split(
        features, target_usage, target_class, test_size=0.2, random_state=42
    )

    # Train models
    log.info("Training RandomForest Regressor...")
    regressor = RandomForestRegressor(n_estimators=100, random_state=42)
    regressor.fit(X_train, y_train_usage)

    log.info("Training RandomForest Classifier...")
    classifier = RandomForestClassifier(n_estimators=100, random_state=42)
    classifier.fit(X_train, y_train_class)

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

    # Output regressor to pipeline output
    with open(regressor_model_path, 'rb') as f:
        with open(output_traffic_regressor_model.path, 'wb') as out_f:
            out_f.write(f.read())

    with open(classifier_model_path, 'rb') as f:
        with open(output_traffic_classifier_model.path, 'wb') as out_f:
            out_f.write(f.read())

    log.info("Traffic predictor models saved and uploaded successfully.")


'''
# ─────────────────────────────────────────────────────────────── #
# RandomForest Traffic Prediction Component (Prediction)

#from kfp.v2.dsl import component, Output, Dataset

@component(
    base_image='python:3.9',
    packages_to_install=['pandas', 'scikit-learn', 'boto3', 'joblib']
)
def predict_traffic_levels_component(
    s3_bucket: str,
    s3_key_prefix: str,
    s3_endpoint: str,
    aws_access_key: str,
    aws_secret_key: str,
    output_traffic_predictions: Output[Dataset]
):
    import pandas as pd
    import joblib
    import boto3
    from io import BytesIO
    import os
    import logging
    from sklearn.preprocessing import LabelEncoder

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

    # Initialize S3 client
    s3 = boto3.client('s3', endpoint_url=s3_endpoint)

    # Load models from S3
    log.info("Loading models from S3...")
    regressor_model_key = f'{s3_key_prefix}/models/traffic_regressor_model.joblib'
    classifier_model_key = f'{s3_key_prefix}/models/traffic_classifier_model.joblib'

    regressor_buffer = BytesIO()
    classifier_buffer = BytesIO()

    s3.download_fileobj(s3_bucket, regressor_model_key, regressor_buffer)
    s3.download_fileobj(s3_bucket, classifier_model_key, classifier_buffer)

    regressor_buffer.seek(0)
    classifier_buffer.seek(0)

    regressor = joblib.load(regressor_buffer)
    classifier = joblib.load(classifier_buffer)

    # Get the latest RAN metrics file
    log.info("Fetching latest RAN metrics file...")
    files = sorted([o['Key'] for o in s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key_prefix).get('Contents', []) if "ran-combined-metrics" in o['Key']])
    if not files:
        raise FileNotFoundError("No RAN metrics files found in S3 bucket.")
    latest_key = files[-1]
    log.info(f"Using latest metrics file: {latest_key}")

    response = s3.get_object(Bucket=s3_bucket, Key=latest_key)

    # Explicitly define expected columns and set them after reading the CSV
    expected_columns = ['CellID', 'Timestamp', 'Band', 'Frequency', 'AreaType', 'AdjacentCells', 
        'RSRP', 'RSRQ', 'SINR', 'Throughput', 'Latency', 'MaxCapacity', 'UEsUsage']
    df = pd.read_csv(BytesIO(response['Body'].read()), header=None, names=expected_columns)

    # Log and inspect the columns
    log.info(f"Assigned columns: {list(df.columns)}")

    # Apply data sanitation and preprocessing
    df['Band'] = df['Band'].str.strip().replace(' ', '').astype(str)
    df['Frequency'] = pd.to_numeric(df['Frequency'], errors='coerce').fillna(0)
    df['AdjacentCells'] = pd.to_numeric(df['AdjacentCells'], errors='coerce').fillna(0)
    df['UEsUsage'] = pd.to_numeric(df['UEsUsage'], errors='coerce').fillna(0)
    
    # Encode categorical variables
    band_encoder = LabelEncoder()
    area_encoder = LabelEncoder()
    df['Band'] = band_encoder.fit_transform(df['Band'])
    df['AreaType'] = area_encoder.fit_transform(df['AreaType'].astype(str))

    # Filter to valid rows
    df = df.dropna(subset=['Band', 'Frequency', 'UEsUsage', 'AreaType', 'AdjacentCells'])
    if df.empty:
        raise ValueError("No valid rows left after cleaning.")

    features = df[['Band', 'Frequency', 'UEsUsage', 'AreaType', 'AdjacentCells']]

    # Perform predictions
    log.info("Running predictions...")
    predicted_traffic = regressor.predict(features)
    predicted_classes = classifier.predict(features)

    df['PredictedTraffic'] = predicted_traffic
    df['TrafficLevel'] = predicted_classes  # 0 = Not Busy, 1 = Busy

    # Save predictions to S3
    traffic_predictions_csv = df.to_csv(index=False)
    predictions_buffer = BytesIO(traffic_predictions_csv.encode('utf-8'))
    predictions_key = f'{s3_key_prefix}/predictions/traffic_predictions.csv'

    s3.upload_fileobj(predictions_buffer, s3_bucket, predictions_key)
    log.info(f"Predictions uploaded to: s3://{s3_bucket}/{predictions_key}")

    # Save predictions to output artifact for Kubeflow
    with open(output_traffic_predictions.path, 'w') as f:
        f.write(traffic_predictions_csv)
    log.info("Predictions written to Kubeflow output artifact.")


# ─────────────────────────────────────────────────────────────── #
# LSTM Prediction Component

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

