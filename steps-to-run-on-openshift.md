# Running the AI-Enabled-RAN Demo on OpenShift

## Table of Contents

1. [Installing a cluster](#1-installing-a-cluster)  
2. [Minimum resource requirements](#2-minimum-resource-requirements)  
3. [Create the working namespace](#3-create-the-working-namespace)  
4. [Prerequisites](#4-prerequisites)  
   - [Upload docs for RAG](#upload-docs-for-rag)  
   - [Configure OpenShift AI](#configure-openshift-ai)  
5. [Deploy RAN Simulator](#5-deploy-ran-simulator)  
6. [Kubeflow Pipelines for continuous flow of RAN Anomaly Detection and Traffic Prediction](#6-kubeflow-pipelines-for-continuous-flow-of-ran-anomaly-detection-and-traffic-prediction)  
7. [Deploy MCP Forecast Agent](#7-deploy-mcp-forecast-agent)  
   - [Create required secrets](#create-required-secrets-1)  
   - [Deployment variables](#deployment-variables)  
   - [Deploying the Forecast Agent](#deploying-the-forecast-agent)  
   - [Verify Forecast Agent is running](#verify-forecast-agent-is-running)  
8. [Deploy RANCHAT](#8-deploy-ranchat)  
   - [Create required secrets](#create-required-secrets)  
   - [Configure Deployment Environment Variables](#configure-deployment-environment-variables)  
   - [Optional: Use External MySQL Database](#optional-use-external-mysql-database)  
     - [Enable MySQL support in the deployment](#enable-mysql-support-in-the-deployment)  
     - [Deploy MySQL to OpenShift](#deploy-mysql-to-openshift)  
   - [Deploy RANCHAT](#deploy-ranchat)  
   - [Verify RANCHAT is running](#verify-ranchat-is-running)

## 1. Installing a cluster

This section provides quick access to the official Red Hat documentation for installing OpenShift Container Platform (OCP) on major public cloud providers. Follow the links below for step-by-step instructions to deploy OpenShift on AWS, Azure, or Google Cloud Platform (GCP) using installer-provisioned infrastructure.
| Cloud | Instruction Link | 
| ------ | ------ |
|    AWS    |   [steps-to-install-ocp-on-aws](https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/installing_on_aws/installer-provisioned-infrastructure#installing-aws-default)     |
|      Azure  |  [steps-to-install-ocp-on-azure](https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/installing_on_azure/installer-provisioned-infrastructure#installing-azure-default)     |
|    GCP   |   [steps-to-install-ocp-on-gcp](https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/installing_on_gcp/installing-gcp-default#prerequisites)     |

## 2. Minimum resource requirements

A compact OpenShift clusters with 3 nodes:

| Topology | Number of Nodes | vCPU         | Memory         | Storage |
|----------|------------------|--------------|----------------|---------|
| Compact  | 3                | 8 vCPU cores | 16 GB of RAM   | 120 GB  |

GPU resources are optional, when using MaaS this demo can run only using CPU resources. 
If the model is served locally a GPU must be provided. From NVIDIA perspectice customer can choose:

- NVIDIA RTX A6000 (48GB vRAM)
- NVIDIA A100 (up to 80GB vRAM)
- NVIDIA L40

Recommended minimum operator resources:

| Operator                    | Minimum Requirements                                               |
|-----------------------------|--------------------------------------------------------------------|
| Red Hat OpenShift AI        | At least 2 worker nodes with at least 8 CPUs and 32 GiB RAM        |
| OpenShift Data Foundation   | Less than 2 vCPU and 120 GB storage per node                       |
| Streams for Apache Kafka    | Less than 2 vCPU and 10 GB of memory                               |

To run the AI-Enabled-RAN demo, ensure the following OpenShift components are installed and operational:

- OpenShift Data Foundation
- OpenShift AI
- Kafka

## 3. Create the working namespace

This namespace will be used to deploy all components related to the demo:

```bash
oc create ns ai-ran-genai
```

## 4. Prerequisites

Once the required OpenShift components are running, you’ll need the following credentials and configuration details:

- **Model**: API key, URL, model name

Obtain the model API key, URL , and model name from MaaS or your local deployment.

- **S3 Storage**: Access key, secret key, bucket name, and host

Create an S3 bucket (which will refer to as `S3_BUCKET`) in ODF and save the name.
You can retrieve the additional necessary variables. Be sure to save this information for future use.

```bash
export S3_KEY=$(oc get secret noobaa-admin -n openshift-storage -o jsonpath="{.data.AWS_ACCESS_KEY_ID}" | base64 -d)
export S3_SECRET_ACCESS_KEY=$(oc get secret noobaa-admin -n openshift-storage -o jsonpath="{.data.AWS_SECRET_ACCESS_KEY}" | base64 -d)
export S3_HOST=$(oc get route s3 -n openshift-storage -o jsonpath='{.spec.host}')
```

### Upload docs for RAG

The documents for RAG are used to enhance the model with additional domain context.
The supported file formats are `.docx`, `.pdf`, `.json`, and `.csv`.
For this demo, you can use the sample documents available in the `docs` folder of this repository.

Once you have your S3 credentials, you can upload the documents to your bucket using the following method:

Please note you need AWS CLI installed (out of scope of those instruction).

```bash
export AWS_ACCESS_KEY_ID=$S3_KEY
export AWS_SECRET_ACCESS_KEY=$S3_SECRET_ACCESS_KEY

aws --endpoint-url $S3_HOST s3 cp ./docs/OpenShift_Container_Platform-4.18-Edge_computing-en-US.pdf s3://$S3_BUCKET/docs/OpenShift_Container_Platform-4.18-Edge_computing-en-US.pdf
aws --endpoint-url $S3_HOST s3 cp ./docs/gnodeb.pdf s3://$S3_BUCKET/docs/gnodeb.pdf
aws --endpoint-url $S3_HOST s3 cp ./docs/ran_metrics_and_anomalies.docx s3://$S3_BUCKET/docs/ran_metrics_and_anomalies.docx
aws --endpoint-url $S3_HOST s3 cp ./docs/cell_config.json s3://$S3_BUCKET/docs/cell_config.json
```

> ⚠️ **Note:** The file must be uploaded as folder *docs* in the S3 Bucket. The RANCHAT process list that index for files to be processed for RAG. Leave it as is in the example above.


### Configure OpenShift AI

Once you have access to OpenShift AI, ensure to configure the following;
- Access the Data science project and select your namespace `ai-ran-genai` for this demo.
- Select Workbenches and create workbench. You can select image `TensorFlow` or image of your choice. Container size `small or medium`.
- Select Pipelines and configure pipeline server in order to use later for kubeflow pipelines. Ensure to provide the S3 Object Storage details to configure pipeline server.

## 5. Deploy RAN Simulator

### Prerequisites
Ensure Kafka cluster and topics are configured. For reference you can use the kafka deployment and topics configuration provided part of this repo. It is located in `kafka` folder. 

RAN simulator simulates a Radio Access Network (RAN) to generate realistic telemetry data, including RAN Key Performance Indicators(KPIs). It's designed to push this data to Apache Kafka topics for real-time processing and analysis. 

Begin by deploying the RAN simulator to generate metrics and stream them into Kafka.

* The repository includes a default `cell_config.json` file with a predefined RAN topology of 100 cell sites for demo.
* To create a custom topology, refer to the `generate_ran_topology` documentation.

```bash
oc apply -f ransim/ransim-deploy.yaml -n ai-ran-genai
```
Once deployed, this deployment will start simulating RAN key metrics and push the data using Kafka topics in real-time.  

## 6. Kubeflow Pipelines for continuous flow of RAN Anomaly Detection and Traffic Prediction

`kubeflow-pipeline/ran-genai-kfp.py` code is based on kubeflow pipelines automates an end-to-end MLOps workflow for Radio Access Network (RAN) performance metrics. It encompasses real-time data ingestion, persistent storage, machine learning model training for traffic prediction, and a robust anomaly detection system powered by both Predictive AI and Generative AI (GenAI) with Retrieval-Augmented Generation (RAG).

The pipeline is designed to process streaming RAN data, identify key performance indicator (KPI) anomalies, provide intelligent explanations, and offer actionable recommendations.

* Use this code `kubeflow-pipeline/ran-genai-kfp.py` to modify the Kafka server, Model API keys, S3 Storage information relevant to your environment.
* Compile the python code to generate the pipeline yaml to upload in OpenShift AI pipelines. For example please refer the generated pipeline yaml `kubeflow-pipeline/ran_multi_prediction_pipeline_with_genai.yaml`
* The generated yaml can be uploaded in OpenShift AI pipelines under the namespace you have created for this demo. 

- Access OpenShift AI Pipelines and Import Pipeline. Provide Pipeline name and upload the generated pipeline yaml based on the output after compiling this python code `kubeflow-pipeline/ran-genai-kfp.py`.

## 7. Deploy MCP Forecast Agent

The MCP Forecast Agent is responsible for handling predictive processing using the trained models based on kubeflow-pipelines stored in S3.

### Create required secrets

The Forecast Agent requires S3 credentials to access the model artifacts. These are stored in a secret named `agent-forecast-secrets`.
You can edit the `secrets.yaml` file, ensuring the values are base64-encoded:

```yaml
data:
  S3_KEY: <base64-encoded-s3-key>
  S3_SECRET_ACCESS_KEY: <base64-encoded-s3-secret>
```

Alternatively, you can create the secret from the command line using:

```bash
oc create secret generic agent-forecast-secrets -n ai-ran-genai \
  --from-literal=S3_KEY=<your-s3-key> \
  --from-literal=S3_SECRET_ACCESS_KEY=<your-s3-secret>
```

### Deployment variables

The `agent-forecast` container uses the following enviroments variables, defined in `deployment.yaml`:

| Variable               | Description                                                                 |
|------------------------|-----------------------------------------------------------------------------|
| `S3_KEY`               | AWS-style access key (fetched from the `agent-forecast-secrets` secret).    |
| `S3_SECRET_ACCESS_KEY` | AWS-style secret key (fetched from the `agent-forecast-secrets` secret).    |
| `S3_HOST`              | URL of the S3-compatible endpoint (e.g., MinIO or OpenShift Storage).       |
| `S3_BUCKET`            | Name of the bucket containing the trained model used for predictions.       |

### Deploying the Forecast Agent

Once the secrets and environment variables are configured, deploy the Forecast Agent:

```bash
oc apply -f agentic_ai/agent_forecast/manifests/
```

### Verify Forecast Agent is running

After deployment verify the pod is running and check the logs to ensure the service is started correctly you should see the following:

```nginx
[06/27/25 22:04:34] INFO     Starting MCP server 'ForecastAgent'  server.py:1358
                             with transport 'streamable-http' on                
                             http://0.0.0.0:5001/mcp/   
```


## 8. Deploy RANCHAT

RANCHAT provides a user interface for interacting with the model and viewing automatically detected network anomalies. Follow the steps below to deploy the RANCHAT service:

### Create required secrets

The secrets define the KEY for the API to access the model API and the S3 object storage.
RANCHAT requires three keys: `API_KEY`, `S3_KEY`, and `S3_SECRET_ACCESS_KEY`. These must be base64-encoded if you are editing the `secrets.yaml` file directly.

To encode the values manually use:

```bash
echo -n 'youkey' | base64
```

You can replace the placeholder values in manifests/secrets.yaml with base64-encoded strings:


```yaml
data:
  API_KEY: <base64-encoded-api-key>
  S3_KEY: <base64-encoded-s3-key>
  S3_SECRET_ACCESS_KEY: <base64-encoded-s3-secret>
```

Once done apply the secrets.yaml file:

```bash
oc apply -n ai-ran-genai -f ranchat/manifests/secrets.yaml
```

Alternatively, you can create the secret directly via the OpenShift CLI without creating it from the yaml:

```bash
oc create secret generic ranchat-secrets -n ai-ran-genai \
  --from-literal=API_KEY=<your-api-key> \
  --from-literal=S3_KEY=<your-s3-key> \
  --from-literal=S3_SECRET_ACCESS_KEY=<your-s3-secret>
```

### Configure Deployment Enviroment Variables

Open `ranchat/manifests/deployment.yaml` and adjust the environment variables for your deployment. These settings control API access, S3 integration, MySQL configuration, and the Forecast Agent URL.

Below is a breakdown of those variables:

| Variable               | Description                                                                 |
|------------------------|-----------------------------------------------------------------------------|
| `API_URL`              | URL of the inference API (e.g., OpenAI-compatible endpoint).                |
| `API_MODEL`            | Model identifier for inference requests.                                    |
| `API_KEY`              | Pulled from secret; authenticates to the API endpoint.                      |
| `S3_KEY`               | Pulled from secret; AWS access key for S3-compatible object storage.         |
| `S3_SECRET_ACCESS_KEY` | Pulled from secret; AWS secret key for object storage.                      |
| `S3_HOST`              | Hostname or IP address of the S3-compatible server (e.g., MinIO).           |
| `S3_BUCKET`            | Target S3 bucket name for storing or retrieving data.                       |
| `USE_MYSQL`            | Set to `"True"` to enable connection to an external MySQL database.         |
| `DB_HOST`              | Hostname or service name of the MySQL database.                             |
| `DB_USER`              | MySQL username.                                                             |
| `DB_PWD`               | MySQL password.                                                             |
| `DB_NAME`              | Target database name.                                                       |
| `FORECAST_AGENT_URL`   | MCP endpoint of the Forecast Agent (leave the default value)                |

### Optional: Use External MySQL Database

To use an external or in-cluster MySQL database with RANCHAT, follow these steps:

#### Enable MySQL support in the deployment

Set the following enviroment variable in `ranchat/manifests/deployment.yaml`:

```yaml
- name: USE_MYSQL
  value: "True"
```

Ensure the other database-related variables are properly configured:

```yaml
- name: DB_HOST
  value: 'mysql-service'
- name: DB_USER
  value: 'root'
- name: DB_PWD
  value: 'rangenai'
- name: DB_NAME
  value: 'ran_events'
```

If you are deploying MySQL to the current OpenShift instead of connecting to an external DB, you can change the `DB_PWD` editing the deployment yaml for MySQL as explained in step B.

#### Deploy MySQL to OpenShift

If you want to deploy a MySQL instance, use the manifests provided in the `ranchat/manifests/mysql` folder.

1. **Change or leave current password** 

   Edit the `ranchat/manifests/mysql/deployment.yaml and change the `MYSQL_ROOT_PASSWORD` variable

2. **Create the persistent volume claim** to provide storage for the MySQL database:

   ```bash
   oc create -f ranchat/manifests/mysql/pvc.yaml -n ai-ran-genai
   ```

3. **Deploy the MySQL resources** services and deployment

   ```bash
   oc create -f ranchat/manifests/mysql/ -n ai-ran-genai
   ```

This will provision a MySQL database and expose it via the service `mysql-service`.

Ensure the PVC is created before applying the MySQL deployment files to avoid pod startup issues due to missing volume claims.

### Deploy RANCHAT

After completing the above steps, you can now deploy the RANCHAT components using:

```bash
oc create -f ranchat/manifests/ -n ai-ran-genai
```

This will deploy the secrets, deployment and services into your OpenShift environment.

### Verify RANCHAT is running

To confirm that RANCHAT is up and accessible, retrieve the OpenShift route:

```bash
oc get routes -n ai-ran-genai
```

Look for the route named `ranchat` in the output this will be the external URL to access the RANCHAT UI.

> ⚠️ **Note:** The initial startup may take a few minutes, especially if a large amount of documentation is being loaded from S3 for the RAG (Retrieval-Augmented Generation) process which is execute at the first initial startup.

You can monitor the pod logs to check the progress. Wait until you see a log message indicating:

```nginx
 * Running on all addresses (0.0.0.0)
 * Running on http://127.0.0.1:5000
 * Running on http://192.168.1.38:5000
Press CTRL+C to quit
```

Once this appears, open the route URL in your browser you should see the RANCHAT web interface load successfully.

![RANCHAT UI](docs/img/ranchat_ui.png?raw=true "RANCHAT UI")


