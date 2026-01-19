# Dataplex GenAI Agents

This directory hosts a collection of AI-powered agents designed to supercharge **Google Cloud Dataplex** workflows. By leveraging **Google's Agent Development Kit (ADK)** and **Gemini models**, these agents automate complex data management tasks, from governance and quality checks to metadata discovery and lineage analysis.

## 🤖 Available Agents

| Agent | Description | Key Tech |
| :--- | :--- | :--- |
| **[Policy-as-Code Agent](./policy-as-code/README.md)** | Defines, validates, and enforces data governance policies using natural language. It translates your instructions (e.g., "All finance tables need descriptions") into executable Python code that runs against Dataplex and BigQuery metadata. | Gemini 2.5, Vector Search, Firestore |

## 🌟 Why use Dataplex Agents?

These agents are designed to bridge the gap between human intent and technical execution in the data management space.

*   **Natural Language Interfaces**: Interact with your data systems using conversational English instead of complex SQL or API calls.
*   **Automation**: Offload repetitive tasks like policy validation, tagging, and documentation to AI.
*   **Extensibility**: Built on the ADK, these agents can be customized and extended to fit your specific organizational needs.
*   **Security**: Designed with "Human-in-the-loop" principles and sandboxed execution environments to ensure safe operations.

## 🛠️ Common Architecture

Most agents in this repository share a similar architectural foundation:

1.  **Agent Development Kit (ADK)**: The core framework for building, testing, and deploying the agents.
2.  **Gemini Models**: The reasoning engine (e.g., Gemini 2.5 Pro/Flash) used for understanding user intent, generating code, and summarizing results.
3.  **Tool Use (Function Calling)**: Agents interact with Google Cloud services (Dataplex, BigQuery, GCS) via defined tools/MCPs.
4.  **Memory**: Integration with services like Firestore or Vector Search to retain context and learn from past interactions.

## 🚀 Getting Started

Each agent is self-contained in its own directory with specific instructions. However, general prerequisites usually include:

*   **Google Cloud Project** with billing enabled.
*   **Python 3.11+**.
*   **Google Cloud SDK** (`gcloud`) installed and authenticated.
*   **Vertex AI** and **Dataplex** APIs enabled.

To get started, navigate to the specific agent's directory:

```bash
cd policy-as-code
# Follow the README.md inside
```

## 🤝 Contributing

We welcome contributions! If you have an idea for a new agent or an improvement to an existing one:

1.  Ensure your agent follows the [ADK standards](https://github.com/google/adk-samples).
2.  Include a comprehensive `README.md` and `requirements.txt`/`pyproject.toml`.
3.  Place your agent in a new subdirectory under `agents/`.
4.  Submit a Pull Request.