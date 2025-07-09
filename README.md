# AI-Enabled RAN

AI-Enabled RAN is a sample implementation to enhance the operational efficiency of RAN by integrating Predictive AI and Generative AI (GenAI) technologies using OpenShift AI. The solution leverages real-time RAN metrics to detect, explain, and proactively remediate network anomalies, drastically reducing time-to-reslution and improving service quality.
Modern mobile networks generate vast volumes of telemetry data, but interpreting and acting on this data quickly and accurately remains a challenge for network engineers. This project showcase how OpenShift AI can transforms RAN operations by:

- Anticipating problems before they impact users through anomaly detection.
  * Fast Response Times, contextual remediation suggesttions reduce mean time to resolution (MTTR).
- Automatic root cause analysis and resolutions steps using LLM enhanced via RAG with specific domain documentation.
  * Explainability: GenAI generates human-like descriptions of network issues, helping less experienced engineers quickly grasp complex problems.
  * Empowering engineers with natural language explanations and actionable recommendations for optimizing upgrade and maintenance schedules.
 
## Key Features

Predictive AI

  - Forecasts cell-level anomalies using trained models
  - Optimizes upgrade and maintenance schedules
  - Detects issues before service degradation impacts end-users

Generative AI

  - Produces natural language explanations for detected anomalies
  - Suggests prescriptive remediation steps
  - Generate code snippets or commands to accelerate issue resolution (ROADMAP)

## Architecture

![RAN GenAI Architecture with OpenShift AI](docs/img/ran_genai_architecture.png?raw=true "RAN GenAI Architecture with OpenShift AI")

AI-driven, automated RAN operations framework built on Red Hat OpenShift, combining near real-time telemetry, predictive modeling, and GenAI to support autonomous RAN monitoring and remediation.

 **RAN Simulator**

 - A RAN simulator continuously emit operational telemetry data.
 - This data is sent via Kafka, which acts as the central data bus for ingesting real-time metrics.
 
 **AI/ML Pipeline**

 - Kubeflow Pipeline:
   * Automates model training workflows.
   * Handles data preprocessing, training, validation, and deployment.
 - Model Training & Anomaly Detection:
   * Uses incoming RAN metrics to train models continuously
   * Detects anomalies (e.g. degradation, congestion) as they arise.
 - Model Storage
   * Trained models are saves as S3 objects for reuse in predictions

 **GenAI Integration**
 - Hosts on ODF predictive models for inference
 - RAG (Retrieval-Augumented Generation):
   * Accesses external documentation and knowledge bases.
   * Enriches LLM responses with factual, context-aware information.
 - Open Source LLM:
   * Generate natual language explanations.
   * Produce prescriptive remendiation steps.

 **MCP Agent/Server**
 - MCP Agent used for traffic forecast
 - Loads trained models from the S3 object store.
 - Performs prediction inference based on the historical data.

 **Web Interface**
 - Simple UI to chat with LLM.
 - Display automatically detected events.

## Components
 - **Red Hat OpenShift AI (RHOAI)**
 - **Red Hat OpenShift Data Foundation (ODF)**
 - **Streams for Apache Kafka**
 - **Hosted Model-as-a-Service** or **Local model serving with RHOAI**


## Demo Scripts

WIP

## Demo Video

WIP

## Repository Structure

Below is the repository structure and each components
.
├── agentic-ai <-- Contains MCP agents
│   └── agent_forecast
│       └── manifests <-- Manifests used to deploy the forecast agent
├── kubeflow-pipeline <-- KubeFlow pipeline used for metrics machine learning and anomaly detection
├── ranchat <-- UI Interface
│   ├── manifests <-- Manifests to deploy ranchat
│   │   └── mysql <-- Example deployment for MariaDB used for storing anomaly events
│   ├── static <-- Assets used by the UI
│   │   ├── css
│   │   └── images
│   └── templates <-- HTML templates
└── ransim <-- RAN topology generator and RAN simulator


## Running the Demo

To test this application, follow the appropriate instructions based on your environment:

- On RHDP: Refer to the instructions [steps-to-run-on-rhdp](./steps-to-run-on-rhdp.md) .

- On On-Premises or Public Cloud OpenShift Cluster: Follow the steps mentioned in [steps-to-run-on-openshift](./steps-to-run-on-openshift.md) file.




