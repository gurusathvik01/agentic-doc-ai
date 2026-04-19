# 🧠 Agentic Document AI System

A modular **Agentic AI pipeline** for document understanding, layout-aware parsing, and intelligent question answering using LLMs.

This project goes beyond simple LLM usage by implementing a **multi-agent architecture** that processes documents, builds context, and generates structured answers.

---

## 🚀 Features

- 📄 **Document Processing**
  - PDF parsing and structured extraction
  - Layout-aware understanding

- 🧠 **Agent-Based Architecture**
  - Query planning
  - Execution orchestration
  - Specialized agents (data, decision, insight, risk)

- 🔍 **Page Indexing (RAG-like system)**
  - Efficient document navigation
  - Context retrieval using PageIndex

- 🤖 **LLM Integration**
  - Local LLM (Ollama)
  - Context-aware responses
  - Multi-step reasoning pipeline

- ⚙️ **Full Stack Setup**
  - Backend APIs
  - Frontend interface
  - Config-driven architecture

---

## 🏗️ Project Structure

agentic-doc-ai/
│
├── backend/ # API routes, agents, DB handling
├── core/ # Orchestration, execution engine, indexing
├── frontend/ # UI layer
├── PageIndex/ # Document indexing & retrieval
├── model/ # Model configs (lightweight)
│
├── app.py # Entry point
├── config_v2.json # System configuration
├── requirements_v2.txt
├── docker-compose.yml
│
├── .env # Environment template
└── README.md

---

## ⚙️ How It Works

1. 📥 **Input Document**  
   User uploads or provides a PDF  

2. 🧩 **Parsing Layer**  
   Extracts structured content with layout awareness  

3. 🗂️ **Indexing Layer**  
   Builds searchable document representation (PageIndex)  

4. 🧠 **Agent Pipeline**  
   Query planner → Orchestrator → Agents  

5. 🤖 **LLM Response**  
   Generates final answer using context  

---

## 🛠️ Tech Stack

- Python  
- LLM: Ollama (local models)  
- Document Processing  
- Custom Agent Framework  
- FastAPI (Backend)  
- Frontend UI  

---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/gurusathvik01/agentic-doc-ai.git
cd agentic-doc-ai
---
2. Install dependencies
pip install -r requirements_v2.txt

```

---
3. Setup Environment

Create a .env file in the root:

MYSQL_PASSWORD=your_actual_password

⚠️ No secrets are stored in this repository

---

##4. Run Backend
   
python app.py

OR

start_backend.bat

---

##5. Run Frontend
   
start_frontend.bat

---

##🔐 Security

No API keys stored in repository
No database credentials exposed
Uses .env for sensitive data
Safe for public GitHub usage

---

##📌 Use Cases

📊 Business report analysis
📚 Research paper querying
📄 Document-based AI assistants
🤖 Intelligent document Q&A systems

---

##🚧 Future Improvements

Multi-document querying
Streaming responses
Improved UI/UX
Advanced agent reasoning

---

##👨‍💻 Author

Guru Sathvik
