import re
import os
import json
import asyncio
import boto3
import pandas as pd
import httpx
import shutil

from datetime import datetime
from ast import literal_eval

from flask import Flask, request, jsonify, render_template

from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, Text, DateTime, select, desc, update
)

from fastmcp import Client

# =========================
# LangChain (MODERN)
# =========================
from langchain_openai import ChatOpenAI

from langchain_community.vectorstores import FAISS
#from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.embeddings import FastEmbedEmbeddings
from langchain_community.document_loaders import PyPDFLoader, Docx2txtLoader

from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_core.documents import Document

from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain

from langchain.tools import Tool
from langchain.agents import initialize_agent, AgentType

from langchain.prompts import ChatPromptTemplate

# =========================
# ENV VARS
# =========================
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
API_MODEL = os.getenv("API_MODEL")

S3_KEY = os.getenv("S3_KEY")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_HOST = os.getenv("S3_HOST")

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PWD = os.getenv("DB_PWD")
DB_NAME = os.getenv("DB_NAME")

FORECAST_AGENT = os.getenv("FORECAST_AGENT_URL")

# =========================
# DATABASE
# =========================
engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PWD}@{DB_HOST}/{DB_NAME}",
    echo=True
)

metadata = MetaData()

events = Table(
    "events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("cell_id", Integer, nullable=False),
    Column("creation_date", DateTime, nullable=False),
    Column("event", Text, nullable=False),
    Column("data", Text),
    Column("active", Integer, server_default="1"),
)

metadata.create_all(engine)

# =========================
# S3 CLIENT
# =========================
s3 = boto3.client(
    "s3",
    verify=False,
    endpoint_url=S3_HOST,
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET_ACCESS_KEY,
)



# =========================
# MCP FORECAST TOOL
# =========================
async def call_forecast(cell_data: list) -> str:
    async with Client(FORECAST_AGENT) as client:
        result = await client.call_tool("forecast", {"cell_data": cell_data})
        return str(result)

forecast_tool = Tool(
    name="forecast",
    func=lambda data: asyncio.run(call_forecast(literal_eval(data))),
    description="Call MCP forecast agent. Input: list of dicts."
)

# =========================
# S3 HELPERS
# =========================
def list_s3_objects(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for page in pages for obj in page.get("Contents", [])]

def download_from_s3(bucket, prefix, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    for key in list_s3_objects(bucket, prefix):
        local_path = os.path.join(local_dir, os.path.relpath(key, prefix))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(bucket, key, local_path)

def upload_to_s3(bucket, prefix, local_dir):
    for root, _, files in os.walk(local_dir):
        for file in files:
            full = os.path.join(root, file)
            s3.upload_file(
                full,
                bucket,
                os.path.join(prefix, os.path.relpath(full, local_dir)),
            )

# =========================
# DB HELPERS
# =========================
def insert_event(event_text, data):
    with engine.begin() as conn:
        conn.execute(
            events.insert().values(
                creation_date=datetime.utcnow(),
                event=event_text,
                data=data,
            )
        )

def fetch_all_events():
    with engine.begin() as conn:
        return conn.execute(
            select(events).order_by(desc(events.c.creation_date)).limit(10)
        ).fetchall()

RAG_PROMPT = ChatPromptTemplate.from_messages([
    ("system",
     "You are a Radio Access Network (RAN) engineer and telecom operations expert. "
     "Use ONLY the provided documentation to answer the question. "
     "If the answer cannot be found in the documents, say you do not know."),
    ("human",
     "Context:\n{context}\n\nQuestion:\n{input}")
])

# =========================
# LLM
# =========================
def load_llm():
    return ChatOpenAI(
        api_key=API_KEY,
        base_url=f"{API_URL}/v1",
        model=API_MODEL,
        temperature=0,
        max_tokens=2000,
    )

# =========================
# DOCUMENT INGESTION
# =========================
def get_rag_docs():
    docs = []
    download_from_s3(S3_BUCKET, "docs/", "/tmp/docs")

    for file in os.listdir("/tmp/docs"):
        path = f"/tmp/docs/{file}"

        if file.lower().endswith(".pdf"):
            docs.extend(PyPDFLoader(path).load())
        elif file.lower().endswith(".docx"):
            docs.extend(Docx2txtLoader(path).load())
        elif file.lower().endswith(".json"):
            with open(path) as f:
                docs.append(
                    Document(
                        page_content=json.dumps(json.load(f), indent=2),
                        metadata={"source": path, "type": "json"},
                    )
                )

    splitter = RecursiveCharacterTextSplitter(chunk_size=1200, chunk_overlap=50)
    return splitter.split_documents(docs)

# =========================
# RAG CHAIN (MODERN)
# =========================
def build_rag_chain(llm):
    embeddings = FastEmbedEmbeddings()#HuggingFaceEmbeddings(
        #model_name="sentence-transformers/all-MiniLM-L6-v2"
    #)

    if list_s3_objects(S3_BUCKET, "faiss_index/"):
        download_from_s3(S3_BUCKET, "faiss_index", "/tmp/faiss")
        vectordb = FAISS.load_local(
            "/tmp/faiss",
            embeddings,
            allow_dangerous_deserialization=True,
        )
    else:
        docs = get_rag_docs()
        vectordb = FAISS.from_documents(docs, embeddings)
        vectordb.save_local("/tmp/faiss")
        upload_to_s3(S3_BUCKET, "faiss_index", "/tmp/faiss")

    retriever = vectordb.as_retriever()

    doc_chain = create_stuff_documents_chain(
        llm=llm,
        prompt=RAG_PROMPT
    )

    rag_chain = create_retrieval_chain(
        retriever=retriever,
        combine_docs_chain=doc_chain
    )

    return rag_chain#create_retrieval_chain(retriever, doc_chain)

# =========================
# ORIGINAL PROMPTS
# =========================
CLASSIFIER_PROMPT = """
You are a strict classifier. Classify the following user question into only one of the following categories:

PREDICTION – The question is about predictions or forecasts.
UPGRADE – The question contains the term "ClusterGroupUpgrade".
GENERAL – The question does not fall into the above categories.

Respond with exactly one word only.

User question: {question}
"""

PREDICTION_PROMPT = """
Extract the Cell ID and the date or day from the user input, and internally convert it into this JSON format:
[{{"Cell ID": "100", "Datetime_ts": "2025-08-04T12:00:00"}}]

Use this JSON to call the forecast agent.

Return a plain English sentence only.

User input: {input}
"""

UPGRADE_PROMPT = """
Return the following YAML populated with the correct clusters:

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
  remediationStrategy:
    maxConcurrency: 1
  enable: true

User input: {input}
"""

#=========================
#FLASK APP
#=========================

app = Flask(__name__)

@app.route("/api/query", methods=["POST"])
async def query_model():
    user_query = request.json["query"]

    classification = rag_chain.invoke({
        "input": CLASSIFIER_PROMPT.format(question=user_query)
    })["answer"]

    if "PREDICTION" in classification:
        result = await agent.arun(
            PREDICTION_PROMPT.format(input=user_query)
        )
        return jsonify({"response": result})

    if "UPGRADE" in classification:
        result = rag_chain.invoke({
            "input": UPGRADE_PROMPT.format(input=user_query)
        })
        return jsonify({"response": result["answer"]})

    result = rag_chain.invoke({"input": user_query})
    return jsonify({"response": result["answer"]})

@app.route("/api/events", methods=["GET"])
def get_events():
    try:
        with engine.begin() as conn:
            stmt = select(events).where(events.c.active == 1).order_by(desc(events.c.creation_date))
            rows = conn.execute(stmt).fetchall()

        return jsonify([
            {
                "id": r.id,
                "cell_id": r.cell_id,
                "date": r.creation_date.isoformat(),
                "event": r.event,
                "data": r.data,
            } for r in rows
        ])
    except Exception as e:
        print("Error on the API: " % str(e))
        return jsonify({"error": str(e)}), 500

    #rows = fetch_all_events()
    #return jsonify([
    #{
    #    "id": r.id,
    #    "date": r.creation_date.isoformat(),
    #    "event": r.event,
    #    "data": r.data,
    #} for r in rows
    #])

@app.route('/api/ran', methods=['POST'])
def get_ran_cmd():
    try:
        data = request.get_json()
        print(f"Received request: {data}")

        if not data or 'cell_id' not in data:
            return jsonify({"error": "Missing 'cell_id' in request body"}), 400

        cell_id = data['cell_id']

        # Update event(s) in DB for this cell_id
        with engine.begin() as conn:
            stmt = (
                update(events)
                .where(events.c.cell_id == cell_id)
                .values(active=0)
            )
            result = conn.execute(stmt)
            print(f"Updated {result.rowcount} row(s) to active=0 for cell_id={cell_id}")

        return jsonify({"message": f"cell_id {cell_id} remediated"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

async def run_agent_test(agent):
    print("test agent")

    prompt = (
        "Use the MCP forecast tool to determine if cell 22 is busy at noon on "
        "August 4, 2025. The input is a list with this format "
        '[{"Cell ID": "100", "Datetime_ts": "2025-08-04T12:00:00"}]'
    )

    response = await agent.ainvoke({"input": prompt})
    print(response["output"])


async def run_anomaly_detection(rag_chain, text_prompt):
    print("Execute sample anomaly detection")
    print("===============================================")

    response = rag_chain.invoke({"input": text_prompt})
    result_text = response["answer"]

    print(result_text)

    if "ANOMALY_DETECT" in result_text:
        print("Anomaly detected – saving event to DB")

        match = re.search(
            r"\*\*\*\* START_EVENT \*\*\*\*(.*?)\*\*\*\* END_EVENT \*\*\*\*",
            result_text,
            re.DOTALL,
        )

        if match:
            extracted_event = (
                match.group(1)
                .strip()
                .replace("ANOMALY_DETECTED", "")
            )

            print(f"Extracted event: {extracted_event}")
            insert_event(extracted_event, sample_data)

        else:
            print("No event block found.")

    print("===============================================")


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

# ==============
# MAIN
# ==============
if __name__ == "__main__":
    llm = load_llm()
    rag_chain = build_rag_chain(llm)

    agent = initialize_agent(
        tools=[forecast_tool],
        llm=llm,
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True,
    )

    async def main():
        await run_agent_test(agent)
        #await run_anomaly_detection(rag_chain, prompt)

    asyncio.run(main())

    app.run(host="0.0.0.0", port=5000, debug=True)

