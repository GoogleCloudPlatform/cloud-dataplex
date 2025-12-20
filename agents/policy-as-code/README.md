# Policy-as-Code Agent

A generative AI-powered agent designed to automate data governance on Google Cloud. It allows users to define, validate, and enforce data policies using natural language queries, translating them into executable code that runs against **Google Cloud Dataplex** and **BigQuery** metadata.

## 🚀 Quick Start

Follow these steps to get the agent up and running locally using the **Agent Development Kit (ADK)** web interface.

### 1. Prerequisites

*   **Python 3.11+**
*   **Google Cloud SDK (`gcloud`)** installed and authenticated.
*   **Git**
*   **uv** (Recommended for dependency management) or standard `pip`.

### 2. Installation

Clone the repository and navigate to the agent's directory:

```bash
# Clone the repository
git clone https://github.com/GoogleCloudPlatform/cloud-dataplex.git
cd cloud-dataplex/agents/policy-as-code
```

Set up a virtual environment and install dependencies.

Using `uv` (Recommended):
```bash
uv sync
```

Using `pip`:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install .
```

### 3. Configuration

The agent requires a few environment variables to function.

1.  Copy the example configuration file:
    ```bash
    cp .env.example .env
    ```

2.  Open `.env` and fill in your details:
    *   `GOOGLE_CLOUD_PROJECT`: Your Google Cloud Project ID.
    *   `GOOGLE_CLOUD_LOCATION`: (e.g., `us-central1`).
    *   `ENABLE_MEMORY_BANK`: Set to `True` to enable long-term memory (requires Firestore). Set to `False` to run without it. See [Memory Integration](./docs/MEMORY_INTEGRATION.md) for details.
    *   `FIRESTORE_DATABASE`: (Optional) Leave as `(default)` unless using a named database.

3.  **Authentication**: Ensure you are authenticated with Google Cloud:
    ```bash
    gcloud auth application-default login
    ```

### 4. Run the Agent

Start the agent using the ADK web interface.

**Using `uv` (Recommended):**
```bash
uv run adk web
```

**Using `pip`:**
```bash
# Ensure your virtual environment is activated
adk web
```

**Optional:** To enable short-term contextual memory (Agent Engine) for better conversation history, add the `--memory_service_uri` flag:

**Using `uv`:**
```bash
uv run adk web --memory_service_uri="agentengine://AGENT_ENGINE_ID"
```

**Using `pip`:**
```bash
adk web --memory_service_uri="agentengine://AGENT_ENGINE_ID"
```

This will start a local web server (usually at `http://localhost:3000` or `http://127.0.0.1:5000`). Open the URL in your browser to chat with the agent!

---

## 💡 Key Features

*   **Natural Language Policies**: "All tables in the finance dataset must have a description."
*   **Hybrid Execution**: Generates Python code on-the-fly for flexibility, but executes it in a sandboxed environment for safety.
*   **Memory & Learning**: Uses **Firestore** and **Vector Search** to remember valid policies. If you ask a similar question later, it reuses the proven code instead of regenerating it.
*   **Dual-Mode Operation**:
    *   **Live Mode**: Queries the **Dataplex Universal Catalog** in real-time.
    *   **Offline Mode**: Analyzes metadata exports stored in **Google Cloud Storage (GCS)**.
*   **Compliance Scorecards**: Run a full health check on your data assets with a single command.
*   **Remediation**: Can suggest specific fixes for identified violations.

## 🏗️ Architecture

The agent is built using the **Google Cloud Agent Development Kit (ADK)** and leverages several Google Cloud services:

*   **Gemini 2.5 Pro**: For complex code generation (converting natural language to Python).
*   **Gemini 2.5 Flash**: For conversational logic, tool selection, and remediation suggestions.
*   **Vertex AI Vector Search**: For semantic retrieval of past policies.
*   **Firestore**: Stores policy definitions, versions, and execution history.
*   **Dataplex API**: For fetching live metadata.

### Project Structure

*   `policy_as_code_agent/`
    *   `agent.py`: Entry point and core agent definition.
    *   `memory.py`: Handles Firestore interactions (saving/retrieving policies).
    *   `llm_utils.py`: Logic for prompting Gemini to generate code.
    *   `simulation.py`: Sandboxed execution engine for running policy code.
    *   `prompts/`: Markdown templates for LLM instructions.
*   `tests/`: Unit and integration tests.
*   `data/`: Sample metadata for local testing.

## 🧪 Running Tests

To run the test suite and ensure everything is working correctly:

**Using `uv` (Recommended):**
```bash
uv run pytest
```

**Using `pip`:**
```bash
pytest
```

## 📚 Documentation

For deep dives into the implementation, check the `docs/` folder:
- [High-Level Architecture](./docs/HIGH_LEVEL_DETAILS.md)
- [Low-Level Implementation](./docs/LOW_LEVEL_DETAILS.md)
- [Memory Implementation](./docs/MEMORY_IMPLEMENTATION.md)