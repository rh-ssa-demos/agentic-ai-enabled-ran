import re
import os
import httpx
import asyncio
import boto3
import shutil
import requests
import json
import pandas as pd
from ast import literal_eval
from pathlib import Path
from botocore.config import Config
from fastmcp import Client
from langchain_community.llms import VLLMOpenAI
from langchain_community.document_loaders import Docx2txtLoader
from langchain_community.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.chains import RetrievalQA
from langchain.tools import Tool
from langchain.schema import Document
from langchain.agents import initialize_agent, AgentType
from flask import Flask, request, jsonify, render_template
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Text, DateTime
from sqlalchemy.sql import select, desc
from datetime import datetime

# API vars
API_URL = os.getenv('API_URL')
API_KEY = os.getenv('API_KEY')
API_MODEL = os.getenv('API_MODEL')

# S3 access vars
S3_KEY = os.getenv('S3_KEY')
S3_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY')
S3_BUCKET = os.getenv("S3_BUCKET")
S3_HOST = os.getenv("S3_HOST")

# DB vars
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PWD = os.getenv('DB_PWD')
DB_NAME = os.getenv('DB_NAME')

FORECAST_AGENT = os.getenv('FORECAST_AGENT_URL')

# DB stuff
USE_MYSQL = True
if USE_MYSQL:
    engine = create_engine('mysql+pymysql://%s:%s@%s/%s' % (DB_USER, DB_PWD, DB_HOST, DB_NAME), echo=True)
else:
    engine = create_engine('sqlite:////tmp/events.db', echo=True)

metadata = MetaData()
# DB table
events = Table(
    'events', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('creation_date', DateTime, nullable=False),
    Column('event', Text, nullable=False),
    Column('data', Text, nullable=True)
)
# create the table if it doesn't exist
metadata.create_all(engine)

s3 = boto3.client("s3", verify=False, endpoint_url=S3_HOST, aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET_ACCESS_KEY)

async def call_forecast(cell_data: list) -> str:
    """Call the remote MCP forecast tool. Pass a list of cell data records."""
    async with Client(FORECAST_AGENT) as client:
        result = await client.call_tool("forecast", {"cell_data": cell_data})
        print("Result: %s" % result)
        return str(result)

forecast_tool = Tool(name="forecast",
                 func=lambda data: asyncio.run(call_forecast(eval(data))),
                 description="Call to remote MCP agent to forecast if a cell is busy based on 'cell_data', Pass a list of dicts."
            )

# S3 related functions
# ==========================
def s3_index_exists(bucket, prefix):
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    except s3.exceptions.ClientError:
        return False

def download_index_from_s3(bucket, prefix, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get('Contents', []):
        s3_key = obj['Key']
        local_path = os.path.join(local_dir, os.path.relpath(s3_key, prefix))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(bucket, s3_key, local_path)

def upload_index_to_s3(bucket, prefix, local_dir):
    for root, _, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_dir)
            s3_key = os.path.join(prefix, relative_path)
            s3.upload_file(local_path, bucket, s3_key)

def list_s3_objects(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    all_keys = []
    for page in pages:
        for obj in page.get('Contents', []):
            all_keys.append(obj['Key'])

    return all_keys
# ==========================


# DB related functions
# ==========================
# Insert an event in the DB
def insert_event(event_text, data):
    with engine.begin() as conn:
        conn.execute(events.insert().values(
            creation_date=datetime.utcnow(),
            event=event_text,
            data=data
        ))

# fetch all events
def fetch_all_events():
    with engine.begin() as conn:
        stmt = select(events).order_by(desc(events.c.creation_date)).limit(10)
        result = conn.execute(stmt).fetchall()
        return result
# ==========================


app = Flask(__name__)

def load_maas():
    # Connect to MaaS
    #custom_http_client = httpx.Client(timeout=httpx.Timeout(120))
    llm = VLLMOpenAI(
        openai_api_key=API_KEY,
        openai_api_base=API_URL+"/v1",
        model_name=API_MODEL,
        max_tokens=2000,
        temperature=0
    )
    return llm


def get_rag_docs():
    all_docs = []
    print("Check if index exists")
    files = list_s3_objects(S3_BUCKET, 'docs/')
    print("Files: %s" % files)
    if files:#s3_index_exists(S3_BUCKET, "docs/gnodeb.pdf"):
        print("docs index found, download to local dir")
        download_index_from_s3(S3_BUCKET, "docs/", "/tmp/docs")
        for filename in os.listdir("/tmp/docs"):
            file_path = os.path.join('/tmp/docs/', filename)
            
            if os.path.isfile(file_path):
                if filename.lower().endswith(".pdf"):
                    print("Detected PDF: %s" % file_path)
                    loader = PyPDFLoader(file_path)
                    loader_docs = loader.load_and_split() 
                elif filename.lower().endswith(".docx"):
                    print("Detected DOCX: %s" % file_path)
                    loader = Docx2txtLoader(file_path)
                    loader_docs = loader.load()
                #elif filename.lower().endswith(".json"):
                #    print("Detected JSON file: %s" % file_path)
                #    with open(file_path, 'r', encoding='utf-8') as f:
                #        data = json.load(f)
                #    json_str = json.dumps(data, indent=2)
                #    print("Load json data and metadata")
                #    loader_docs = [Document(page_content=json_str, metadata={"source": file_path, "type": "json"} )]
                # add all docs
                all_docs += loader_docs
        # split documents into chunks for embeddings
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1200, chunk_overlap=50)
        docs = text_splitter.split_documents(all_docs)
        return docs

def load_csv():
    files = list_s3_objects(S3_BUCKET, 'docs/')
    print("Files: %s" % files)
    loader_docs = []
    if files:
        print("docs index found, download to local dir")
        download_index_from_s3(S3_BUCKET, "docs/", "/tmp/docs")
        for filename in os.listdir("/tmp/docs"):
            file_path = os.path.join('/tmp/docs/', filename)
            if os.path.isfile(file_path):
                if filename.lower().endswith(".csv"):
                    df = pd.read_csv(file_path)
                    for _, row in df.iterrows():
                        bands = [b.strip() for b in row["bands"].split(",")] if pd.notna(row["bands"]) else [] 
                        adjacent_cells = ([int(cell.strip()) for cell in str(row["adjacent_cells"]).split(",")] if pd.notna(row["adjacent_cells"]) and row["adjacent_cells"] != "" else [])

                        content = (
                            f"Cell {row['cell_id']} in city {row['city']} is an {row['area_type']} area "
                            f"with capacity {row.get('max_capacity', 'unknown')} and bands: {', '.join(bands)}."
                        )
                        metadata = {
                            "cell_id": row["cell_id"],
                            "lat": row["lat"],
                            "lon": row["lon"],
                            "bands": bands,
                            "adjacent_cells": adjacent_cells,
                            "area_type": row["area_type"],
                            "city": row["city"],
                            "source": file_path
                        }
                        loader_docs.append(Document(page_content=content, metadata=metadata))
                        print("Loaded CSV and metadata:", content)
                    
                    return loader_docs


def get_chain(llm):
    # create FAISS
    embedding = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
    faiss_index = list_s3_objects(S3_BUCKET, 'faiss_index/')
    print("FAISS files: %s" % faiss_index)
    #faiss_index = []
    if faiss_index != []:
        print("Loading FAISS index from S3...")
        download_index_from_s3(S3_BUCKET, "faiss_index", "/tmp/faiss_index")
        vectordb = FAISS.load_local("/tmp/faiss_index", embeddings=embedding, allow_dangerous_deserialization=True)
    else:
        print("FAISS index not found on S3. Creating a new one...")
        print("Get RAG docs")
        docs = get_rag_docs()
        print("Get JSON for RAG")
        csv_doc = load_csv()
        vectordb = FAISS.from_documents(docs, embedding)
        vectordb.add_documents(json_docs)
        vectordb.save_local("/tmp/faiss_index")

        print("Upload index to S3")
        upload_index_to_s3(S3_BUCKET, "faiss_index", "/tmp/faiss_index")

    #print("VectorDB debug")
    #print(f"Number of docs in FAISS: {len(vectordb.docstore._dict)}")
    #print("Documents ID: %s" % vectordb.docstore._dict.keys())

    print("Set retriever")
    retriever = vectordb.as_retriever()
    print("Create qa_chain")
    qa_chain = RetrievalQA.from_chain_type(llm=llm, chain_type="stuff", retriever=retriever, return_source_documents=True)

    return qa_chain


# Set benchmark prompt
sample_data = """
Cell ID Datetime    Band    RSRP    RSRQ    SINR    Throughput (Mbps)   Latency (ms)    Area Type   Max Capacity
0   2025-04-23 21:12:52 Band 66 -84.36  -11.1   14.98   20.91   21.44   commercial  93
1   2025-04-23 21:12:52 Band 26 -104.18 -5.85   8.12    76.71   20.66   residential 89
2   2025-04-23 21:12:52 Band 29 -83.97  -6.04   17.69   123.13  73.71   industrial  94
3   2025-04-23 21:12:52 Band 29 -97.23  -10.41  17.31   87.41   67.3    commercial  79
4   2025-04-23 21:12:52 Band 29 -116.26 -7.72   6.66    78.57   21.9    industrial  88
5   2025-04-23 21:14:52 Band 29 -118.92 -18.42  -5.11   3.25    88.7    industrial  95
6   2025-04-23 21:14:52 Band 71 -110.24 -6.33   24.66   127.99  30.18   residential 84
7   2025-04-23 21:14:52 Band 26 -101.57 -13.9   18.67   83.65   65.81   commercial  87
8   2025-04-23 21:14:52 Band 66 -102.33 -6.05   2.01    1.88    96.1    commercial  100
9   2025-04-23 21:14:52 Band 26 0   0   0   0   0   industrial  88
"""
prompt = """You are a Radio Access Network (RAN) engineer and telecom operations expert. You have access to the technical documentation and manuals for Baicells BaiBNQ gNodeBs used in a 5G cellular network.
Your task is to detect and report anomalies in the provided network performance dataset. Use your technical knowledge and refer strictly to the Baicells documentation to:

Identify any anomalies in each row based on key metrics: RSRP, RSRQ, SINR, Throughput, and Latency.
Compare performance across cells, especially on the same band or similar area type, to find inconsistencies.

Wrap the next information with the string **** START_EVENT ****

List each anomaly clearly using this format:
Cell ID X, Band Y shows Z (reason for anomaly).

After that, state the name of the Baicells documentation used as reference.
For each anomaly, recommend troubleshooting or remediation steps from the documentation and specify the exact page number and section where relevant settings can be changed.

If the documentation does not cover a specific anomaly or fix, reply with: "I don't know based on the documents provided."

Close this section with the string **** END_EVENT ****

Always prioritize and only use the provided documentation for technical justification.
If any anomaly is found, always begin your response with ANOMALY_DETECTED. If none are found, respond with NO_ANOMALY.

Input format:
A table of network performance data, including Cell ID, Timestamp, Band, RSRP, RSRQ, SINR, Throughput (Mbps), Latency (ms), Area Type, and Max Capacity.

%s
""" % sample_data

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/query', methods=['POST'])
async def query_model():
    data = request.get_json()
    
    if not data or 'query' not in data:
        return jsonify({"error": "Missing 'query' in request body"}), 400

    # perform classification
    prompt = """ Classify the following user question into one of the two categories below:
                 1. Prediction related - Queries about predictions, trends, forecasts, or predictive models
                 2. Upgrade - Any query that includes the word ClusterGroupUpgrade
                 3. General - All other queries not related to predictions or upgrade
                 Answer only PREDICTION or GENERAL or UPGRADE based on the classified category, no other text should be included in the response.
                
                 User question: %s""" % (data['query'])
    response = qa_chain.invoke(prompt)
    print("Response: <br/> %s" % response['result'])

    if 'PREDICTION' in response['result']:
        # extract data
        #prompt = """Based on the user input can you extract the CELL Id and add the date or day and put it in a JSON structure like this: {"cells": "LIST_OF_CELLS", "date": "TIMESTAMP"}, you should only answer with the filled in JSON and nothing else in the response. The TIMESTAMP must always be a date in the future.
                    #User input: %s""" % data['query']
        llm_prompt = "This is a prediction request extract the Cell IDs are needed and the correct date requested by the user. User input: %s" % data['query']
        response = qa_chain.invoke(llm_prompt)

        prompt = """Based on the user input can you extract the CELL Id and add the date or day and put it in a JSON structure using this format [{"Cell ID": "100", "Datetime_ts": "2025-08-04T12:00:00"}] then call the forecast agent to get a response about the prediction.
                    User input: %s""" % response['result']
        data_resp = await agent.arun(prompt)
        print("Agent Response: %s" % data_resp)

        return jsonify({"response": data_resp}), 200

    elif 'UPGRADE' in response['result']:
        print("Upgrade request")
        prompt = """I will provide you the user input and your response should include the YAML template below replacing the [cell-site-X] with the correct name of the cell sites.
                    User input: %s
                    YAML Template:
```yaml
apiVersion: ran.openshift.io/v1alpha1
kind: ClusterGroupUpgrade
metadata:
  name: cgu-platform-operator-upgrade-prep
  namespace: default
spec:
  managedPolicies:
  - du-upgrade-platform-upgrade-prep
  - du-upgrade-platform-upgrade
  clusters:
  - cell-site-1
  - cell-site-2
  - cell-site-3
  remediationStrategy:
    maxConcurrency: 1
  enable: true```""" % data['query']
        data_resp = qa_chain.invoke(prompt)
        print("Response: %s" % data_resp['result'])
        return jsonify({"response": data_resp['result']}), 200
    
    else:

        try:
            #prompt = """You are a Radio Access Network (RAN) engineer and telecom operations expert. You have access to the technical documentation and manuals for Baicells BaiBNQ gNodeBs used in a 5G cellular network and also to the OpenShift Documentation to upgrade clusters using Topology Aware Lifecycle Manager. You also have access to the cell sites topology data so you have full visibility of the RAN topology and if needed you can access that information. Use your knowledge of RAN and the documentation available including the network topology to answer the question provided by the user.
            #User Question: %s
            #""" % data['query']
            prompt = data['query']
            response = qa_chain.invoke(prompt)
            return jsonify({"response": response['result']}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

@app.route('/api/events', methods=['GET'])
def get_events():
    try:
        db_events = fetch_all_events()
        events_list = [
            {
                "id": row.id,
                "creation_date": row.creation_date.strftime("%Y-%m-%d %H:%M:%S"),
                "event": row.event,
                "data": row.data
            }
            for row in db_events
        ]
        return jsonify(events_list), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':

    llm = load_maas()
    qa_chain = get_chain(llm)

    agent = initialize_agent(
        tools=[forecast_tool],
        llm=llm,
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True
    )

    async def run_agent_test():
        print("test agent")
        prompt = 'Use the MCP forecast tool to determine if cell 22 is busy at noon on August 4, 2025. The input is a list with this format [{"Cell ID": "100", "Datetime_ts": "2025-08-04T12:00:00"}]'
        response = await agent.arun(prompt)
        print(response)

    asyncio.run(run_agent_test())

    print("Execute sample anomaly detection")
    print("===============================================")
    response = qa_chain.invoke(prompt)
    print(response['result'])
    if 'ANOMALY_DETECT' in response['result']:
        print("Anamoly detected save event to the DB")
        # extract text between the START_EVENT and END_EVENT markers
        match = re.search(r'\*\*\*\* START_EVENT \*\*\*\*(.*?)\*\*\*\* END_EVENT \*\*\*\*', response['result'], re.DOTALL)

        if match:
            extracted_event = match.group(1).strip().replace('ANOMALY_DETECTED', '')
            print("Extracted event: %s" % extracted_event)
            insert_event(extracted_event, sample_data)
        else:
            print("No event block found.")
    print("===============================================")

    app.run(host='0.0.0.0', debug=True, use_reloader=False)

