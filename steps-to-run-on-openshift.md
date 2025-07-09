# Running the AI-Enabled-RAN Demo on OpenShift

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

