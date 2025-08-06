# Running the AI-Enabled-RAN Demo on OpenShift

## 1. Installing a cluster on Public Cloud
This section provides quick access to the official Red Hat documentation for installing OpenShift Container Platform (OCP) on major public cloud providers. Follow the links below for step-by-step instructions to deploy OpenShift on AWS, Azure, or Google Cloud Platform (GCP) using installer-provisioned infrastructure.
| Cloud | Instruction Link | 
| ------ | ------ |
|    AWS    |   [steps-to-install-ocp-on-aws](https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/installing_on_aws/installer-provisioned-infrastructure#installing-aws-default)     |
|      Azure  |  [steps-to-install-ocp-on-azure](https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/installing_on_azure/installer-provisioned-infrastructure#installing-azure-default)     |
|    GCP   |   [steps-to-install-ocp-on-gcp](https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/installing_on_gcp/installing-gcp-default#prerequisites)     |

## 2. Minimum resource requirements

- Recommended minimum cluster resources:


   | Topology | Number of master nodes       | Number of worker nodes | vCPU    | Memory  | Storage       |
    |-----------------|----------------|----------------|----------------|----------------|----------------|
     | Compact       | 3              |    0 or 1            | 8vCPU cores              |      16GB of RAM          | 120GB           |    

- Recommended minimum operator resources:


   | Operator |                        Minmum requirements |
    | ------             |          ------ |
    |   RHOAI            |     At least 2 worker nodes with at least 8 CPUs and 32 GiB RAM available.    |   
    |   ODF           |    Less than 2 vCPU and 120GB storage per Node     | 
    |   Kafka           |    Less than 2 vCPU and 10GB of Memory     |  


To run the AI-Enabled-RAN demo, ensure the following OpenShift components are installed and operational:

- OpenShift Data Foundation
- OpenShift AI
- Kafka

## 3. Create the Working Namespace

This nameapce will be used to deploy all components related to the demo:

```bash
# oc create ns ai-ran-genai
```

## 4. Prerequisite Variables

Once the required OpenShift components are running, you’ll need the following credentials and configuration details:

- **Model**: API key, URL, model name
- **S3 Storage**: Access key, secret key, bucket name, and host

Follow the instructions here to create and configure the S3 bucket.
**Model Access**: Obtain the model API key, URL , and model name from MaaS or your local deployment.

Once you have access to OpenShift AI, ensure to configure the following;
- Access the Data science project and select your namespace `ai-ran-genai` for this demo.
- Select Workbenches and create workbench. You can select image `TensorFlow` or image of your choice. Container size `small or medium`.
- Select Pipelines and configure pipeline server in order to use later for kubeflow pipelines. Ensure to provide the S3 Object Storage details to configure pipeline server.

## 5. Deploy RAN Simulator 

Begin by deploying the RAN simulator to generate metrics and stream them into Kafka.

* The repository includes a default `cell_config.json` file with a predefined RAN topology of 100 cell sites.
* To create a custom topology, refer to the `generate_ran_topology` documentation.

## Kubeflow Pipelines for continuous flow of RAN Anomaly Detection and Traffic Prediction

`kubeflow-pipeline/ran-genai-kfp.py` code is based on kubeflow pipelines automates an end-to-end MLOps workflow for Radio Access Network (RAN) performance metrics. It encompasses real-time data ingestion, persistent storage, machine learning model training for traffic prediction, and a robust anomaly detection system powered by both Predictive AI and Generative AI (GenAI) with Retrieval-Augmented Generation (RAG).

The pipeline is designed to process streaming RAN data, identify key performance indicator (KPI) anomalies, provide intelligent explanations, and offer actionable recommendations.

* Use this code `kubeflow-pipeline/ran-genai-kfp.py` to modify the Kafka server, Model API keys, S3 Storage information relevant to your environment.
* Compile the python code to generate the pipeline yaml to upload in OpenShift AI pipelines. For example please refer the generated pipeline yaml `kubeflow-pipeline/ran_multi_prediction_pipeline_with_genai.yaml`
* The generated yaml can be uploaded in OpenShift AI pipelines under the namespace you have created for this demo. 

- Access OpenShift AI Pipelines and Import Pipeline. Provide Pipeline name and upload the generated pipeline yaml based on the output after compiling this python code `kubeflow-pipeline/ran-genai-kfp.py`.


