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

## Create the Working Namespace

This nameapce will be used to deploy all components related to the demo:

```bash
# oc create ns ai-ran-genai
```

## Prerequisite Variables

Once the required OpenShift components are running, you’ll need the following credentials and configuration details:

- **Model**: API key, URL, model name
- **S3 Storage**: Access key, secret key, bucket name, and host

Follow the instructions here to create and configure the S3 bucket.
**Model Access**: Obtain the model API key, URL , and model name from MaaS or your local deployment.

## Deploy RAN Simulator 

Begin by deploying the RAN simulator to generate metrics and stream them into Kafka.

* The repository includes a default `cell_config.json` file with a predefined RAN topology of 100 cell sites.
* To create a custom topology, refer to the `generate_ran_topology` documentation.

