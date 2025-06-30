# AI-Enabled RAN

AI-Enabled RAN is a sample implementation to enhance the operational efficiency of RAN by integrating Predictive AI and Generative AI (GenAI) technologies using OpenShift AI. The solution leverages real-time RAN metrics to detect, explain, and proactively remediate network anomalies, drastically reducing time-to-reslution and improving service quality.
Modern mobile networks generate vast volumes of telemetry data, but interpreting and acting on this data quickly and accurately remains a challenge for network engineers. This project showcase how OpenShift AI can transforms RAN operations by:

- Anticipating problems before they impact users through anomaly detection.
- Automatic root cause analysis and resolutions steps using LLM enhanced via RAG with specific domain documentation
- Empowering engineers with natural language explanations and actionable recommendations for optimizing upgrade and maintenance schedules
- 
This project brings automation and intelligenge enabling:

- Faster Response Times: Near real-time anomaly detction and contextual remediation suggestions reduce mean time to resolution (MTTR).
- Explainability: GenAI generates human-like descriptions of network issues, helping less experienced engineers quickly grasp complex problems.
- Operational Agility: Recommendations include not only diagnosis but also a prescriptive action such as what configuration to change and what command to run.

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

![RAN GenAI Architecture with OpenShift AI](img/ran_genai_architecture.png?raw=true "RAN GenAI Architecture with OpenShift AI")

## Components


