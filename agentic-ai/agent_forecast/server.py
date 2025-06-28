from fastmcp import FastMCP
from fastapi.responses import JSONResponse
import pandas as pd
import joblib
import boto3
import os

S3_KEY = os.getenv('S3_KEY')
S3_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY')
S3_BUCKET = os.getenv("S3_BUCKET")
S3_HOST = os.getenv("S3_HOST")

# load models from S3
s3 = boto3.client("s3", verify=False, endpoint_url=S3_HOST, aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET_ACCESS_KEY)
print('Download regressor model')
s3.download_file(S3_BUCKET, 'ran-pipeline/models/traffic_regressor_model.joblib', '/tmp/traffic_regressor_model.joblib')
print('Download classifier model')
s3.download_file(S3_BUCKET, 'ran-pipeline/models/traffic_classifier_model.joblib', '/tmp/traffic_classifier_model.joblib')

model = joblib.load("/tmp/traffic_classifier_model.joblib")
model2 = joblib.load("/tmp/traffic_regressor_model.joblib")

mcp = FastMCP("ForecastAgent")

UE_USAGE_THRESHOLD = 80

@mcp.tool()
def forecast(cell_data: list) -> dict:
    df = pd.DataFrame(cell_data)
    print("Called forecast")
    print("Cell data: %s" % cell_data)

    if "Datetime_ts" in df.columns:
        df["Datetime_ts"] = pd.to_datetime(df["Datetime_ts"])
        df["Hour"] = df["Datetime_ts"].dt.hour
        df["DayOfWeek"] = df["Datetime_ts"].dt.dayofweek
        # convert datetime to numeric timestamp in seconds as float
        df["Datetime_ts"] = df["Datetime_ts"].view('int64') / 1e9

    # predict UEs usage using the regressor model
    print("Features in regressor model: ")
    print(model2.feature_names_in_)
    reg_input_features = list(model2.feature_names_in_)
    for feature in reg_input_features:
        if feature not in df.columns:
            df[feature] = 0
    reg_input = df[reg_input_features].astype(float)
    predicted_usage = model2.predict(reg_input)[0] * 100
    df["UEs Usage"] = predicted_usage  # inject into df
    print("Predicted UEs usage: %s" % predicted_usage) 

    # prepare the classifier input for determine cell are busy or not
    expected_features = list(model.feature_names_in_)
    print(model.feature_names_in_)
    for feature in expected_features:
        if feature not in df.columns:
            df[feature] = 0
    model_input = df[expected_features].astype(float)    

    print("Perform prediction")
    prediction = model.predict(model_input)
    probability = model.predict_proba(model_input)[:, 1]
    is_busy = df["UEs Usage"] > UE_USAGE_THRESHOLD

    print("collect data")
    results = []
    for i, row in df.iterrows():
        prediction_res = {
                "cell_id": row["Cell ID"],
                "cell_busy": bool(prediction[i]),
                "confidence": round(probability[i], 4),
                "predicted_ues_usage": round(row["UEs Usage"], 2),
                "threshold": UE_USAGE_THRESHOLD
            }
        results.append(prediction_res)

    print("Prediction result: %s" % results)
    return results

if __name__ == "__main__":
    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=5001,
        path="/mcp",
        log_level="debug"
    )
