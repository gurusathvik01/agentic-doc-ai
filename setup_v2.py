#!/usr/bin/env python3
"""
Agentic Doc AI v2 Setup Script

This script sets up the production-ready system with all dependencies,
configurations, and initial data.
"""

import os
import sys
import subprocess
import json
from pathlib import Path

def run_command(command, description, check=True):
    """Run a command and handle errors"""
    print(f"\n🔧 {description}...")
    print(f"Running: {command}")
    
    try:
        result = subprocess.run(command, shell=True, check=check, capture_output=True, text=True)
        if result.stdout:
            print(f"✅ Success: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}")
        if e.stderr:
            print(f"Error details: {e.stderr}")
        return False

def check_prerequisites():
    """Check if prerequisites are installed"""
    print("🔍 Checking prerequisites...")
    
    prerequisites = {
        "Python": "python --version",
        "Node.js": "node --version",
        "Ollama": "ollama --version"
    }
    
    missing = []
    for name, command in prerequisites.items():
        try:
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            print(f"✅ {name}: {result.stdout.strip()}")
        except:
            print(f"❌ {name}: Not found")
            missing.append(name)
    
    if missing:
        print(f"\n⚠️  Missing prerequisites: {', '.join(missing)}")
        print("Please install missing prerequisites before continuing.")
        return False
    
    return True

def setup_python_environment():
    """Set up Python virtual environment and dependencies"""
    print("\n🐍 Setting up Python environment...")
    
    # Create virtual environment if it doesn't exist
    if not os.path.exists("venv_v2"):
        run_command("python -m venv venv_v2", "Creating virtual environment")
    
    # Activate virtual environment and install dependencies
    if os.name == 'nt':  # Windows
        activate_cmd = "venv_v2\\Scripts\\activate &&"
        pip_cmd = "venv_v2\\Scripts\\pip"
    else:  # Unix/Mac
        activate_cmd = "source venv_v2/bin/activate &&"
        pip_cmd = "venv_v2/bin/pip"
    
    # Upgrade pip
    run_command(f"{pip_cmd} install --upgrade pip", "Upgrading pip")
    
    # Install requirements
    run_command(f"{pip_cmd} install -r requirements_v2.txt", "Installing Python dependencies")
    
    # Install PageIndex dependencies
    if os.path.exists("PageIndex"):
        os.chdir("PageIndex")
        if os.path.exists("requirements.txt"):
            run_command(f"{pip_cmd} install -r requirements.txt", "Installing PageIndex dependencies")
        os.chdir("..")
    
    return True

def setup_ollama_models():
    """Download and setup Ollama models"""
    print("\n🤖 Setting up Ollama models...")
    
    models = ["llama3.2", "nomic-embed-text"]
    
    for model in models:
        print(f"📥 Downloading {model}...")
        result = subprocess.run(f"ollama pull {model}", shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {model} downloaded successfully")
        else:
            print(f"❌ Failed to download {model}: {result.stderr}")
    
    return True

def setup_directories():
    """Create necessary directories"""
    print("\n📁 Creating directories...")
    
    directories = [
        "uploads",
        "data",
        "logs",
        "cache",
        "reports"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✅ Created directory: {directory}")
    
    return True

def create_config_files():
    """Create configuration files"""
    print("\n⚙️ Creating configuration files...")
    
    # Environment file
    env_content = """# Agentic Doc AI v2 Configuration

# Database Configuration
MONGO_URI=mongodb://localhost:27017/
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASSWORD=${MYSQL_PASSWORD}
MYSQL_DB=agentic_ai

# Ollama Configuration
OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=llama3.2
OLLAMA_EMBED_MODEL=nomic-embed-text

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000

# PageIndex Configuration
PAGEINDEX_PATH=./PageIndex

# Logging
LOG_LEVEL=INFO
LOG_FILE=logs/agentic_ai.log
"""
    
    with open(".env_v2", "w") as f:
        f.write(env_content)
    print("✅ Created .env_v2")
    
    # Configuration JSON
    config = {
        "system": {
            "name": "Agentic Doc AI v2",
            "version": "2.0.0",
            "environment": "production"
        },
        "llm": {
            "provider": "ollama",
            "model": "llama3.2",
            "embed_model": "nomic-embed-text",
            "temperature": 0.7,
            "max_tokens": 4096
        },
        "databases": {
            "mongodb": {
                "uri": "mongodb://localhost:27017/",
                "database": "agentic_ai"
            },
            "mysql": {
                "host": "localhost",
                "user": "root",
                "password": "${MYSQL_PASSWORD}",
                "database": "agentic_ai"
            }
        },
        "pageindex": {
            "path": "./PageIndex",
            "model": "llama3.2",
            "max_pages_per_node": 10,
            "max_tokens_per_node": 2000
        },
        "ui": {
            "theme": "modern",
            "max_history": 100,
            "auto_refresh": True
        }
    }
    
    with open("config_v2.json", "w") as f:
        json.dump(config, f, indent=2)
    print("✅ Created config_v2.json")
    
    return True

def create_startup_scripts():
    """Create startup scripts"""
    print("\n🚀 Creating startup scripts...")
    
    # Backend startup script
    backend_script = """#!/bin/bash
# Agentic Doc AI v2 Backend Startup

echo "🚀 Starting Agentic Doc AI v2 Backend..."

# Activate virtual environment
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    source venv_v2/Scripts/activate
else
    source venv_v2/bin/activate
fi

# Set environment variables
export $(cat .env_v2 | xargs)

# Start backend
cd backend
python main_v2.py

echo "✅ Backend started on http://localhost:8000"
"""
    
    with open("start_backend.sh", "w") as f:
        f.write(backend_script)
    
    # Windows version
    backend_bat = """@echo off
REM Agentic Doc AI v2 Backend Startup

echo 🚀 Starting Agentic Doc AI v2 Backend...

REM Activate virtual environment
call venv_v2\\Scripts\\activate

REM Start backend
cd backend
python main_v2.py

echo ✅ Backend started on http://localhost:8000
pause
"""
    
    with open("start_backend.bat", "w") as f:
        f.write(backend_bat)
    
    # Frontend startup script
    frontend_script = """#!/bin/bash
# Agentic Doc AI v2 Frontend Startup

echo "🎨 Starting Agentic Doc AI v2 Frontend..."

# Activate virtual environment
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    source venv_v2/Scripts/activate
else
    source venv_v2/bin/activate
fi

# Start frontend
cd frontend
streamlit run app_v2.py --server.port=8501 --server.address=0.0.0.0

echo "✅ Frontend started on http://localhost:8501"
"""
    
    with open("start_frontend.sh", "w") as f:
        f.write(frontend_script)
    
    # Windows version
    frontend_bat = """@echo off
REM Agentic Doc AI v2 Frontend Startup

echo 🎨 Starting Agentic Doc AI v2 Frontend...

REM Activate virtual environment
call venv_v2\\Scripts\\activate

REM Start frontend
cd frontend
streamlit run app_v2.py --server.port=8501 --server.address=0.0.0.0

echo ✅ Frontend started on http://localhost:8501
pause
"""
    
    with open("start_frontend.bat", "w") as f:
        f.write(frontend_bat)
    
    print("✅ Created startup scripts")
    return True

def create_documentation():
    """Create documentation"""
    print("\n📚 Creating documentation...")
    
    readme_content = """# Agentic Doc AI v2

Production-ready AI system for multi-dataset querying with PageIndex integration.

## 🏗️ Architecture

```
User Query
    ↓
Ollama LLM (Intent)
    ↓
Ollama Embeddings
    ↓
Page Index (GitHub)
    ↓
Execution Planner (JSON output)
    ↓
Query Engine
   ↙        ↘
MySQL     MongoDB    Docs
    ↓         ↓         ↓
        Merge Layer
             ↓
     Ollama LLM (Answer)
             ↓
          Response
```

## 🚀 Quick Start

1. **Install Dependencies**
   ```bash
   python setup_v2.py
   ```

2. **Start Backend**
   ```bash
   # On Windows
   start_backend.bat
   
   # On Unix/Mac
   ./start_backend.sh
   ```

3. **Start Frontend**
   ```bash
   # On Windows
   start_frontend.bat
   
   # On Unix/Mac
   ./start_frontend.sh
   ```

4. **Access the Application**
   - Frontend: http://localhost:8501
   - Backend API: http://localhost:8000
   - API Docs: http://localhost:8000/docs

## 📊 Features

- **Multi-Dataset Support**: MySQL, MongoDB, PDF, CSV, and other documents
- **Intelligent Query Processing**: Intent recognition and execution planning
- **Real-time Results**: Live query processing with progress tracking
- **JSON Reports**: Comprehensive query reports with execution details
- **Modern UI**: Responsive Streamlit interface with real-time analytics
- **PageIndex Integration**: Advanced document indexing and search

## 🛠️ Configuration

Edit `.env_v2` to configure:
- Database connections
- Ollama models
- API settings
- Logging preferences

## 📁 Project Structure

```
agentic-doc-ai/
├── core/                   # Core system components
│   ├── orchestrator.py     # Main query orchestrator
│   ├── llm_engine.py       # Ollama LLM integration
│   ├── page_index_engine.py # PageIndex integration
│   ├── execution_planner.py # Query execution planning
│   ├── query_engine.py     # Multi-dataset queries
│   └── merge_layer.py      # Result merging
├── backend/
│   └── main_v2.py         # FastAPI backend
├── frontend/
│   └── app_v2.py          # Streamlit frontend
├── PageIndex/             # PageIndex submodule
├── uploads/               # Document uploads
├── data/                  # Data storage
├── logs/                  # System logs
└── reports/               # Generated reports
```

## 🔧 API Endpoints

- `POST /query` - Process queries
- `POST /upload-and-index` - Upload and index documents
- `GET /sources` - Get available data sources
- `GET /system-status` - System health check
- `GET /report/{query_id}` - Generate JSON reports

## 📈 Monitoring

- System status dashboard
- Query performance metrics
- Data source usage analytics
- Real-time processing tracking

## 🤖 Supported Data Sources

- **MySQL**: Relational database queries
- **MongoDB**: NoSQL document queries
- **PDF Documents**: Indexed with PageIndex
- **Markdown**: Indexed with PageIndex
- **CSV Files**: Tabular data processing
- **API Data**: Real-time data integration

## 📝 License

MIT License - see LICENSE file for details.
"""
    
    with open("README_v2.md", "w") as f:
        f.write(readme_content)
    
    print("✅ Created README_v2.md")
    return True

def main():
    """Main setup function"""
    print("🚀 Agentic Doc AI v2 Setup")
    print("=" * 50)
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Setup failed due to missing prerequisites")
        return False
    
    # Setup Python environment
    if not setup_python_environment():
        print("\n❌ Python environment setup failed")
        return False
    
    # Setup Ollama models
    if not setup_ollama_models():
        print("\n⚠️  Ollama model setup failed (you can run this later)")
    
    # Create directories
    setup_directories()
    
    # Create configuration files
    create_config_files()
    
    # Create startup scripts
    create_startup_scripts()
    
    # Create documentation
    create_documentation()
    
    print("\n" + "=" * 50)
    print("✅ Agentic Doc AI v2 setup completed successfully!")
    print("\n🚀 Next steps:")
    print("1. Start the backend: ./start_backend.sh (or start_backend.bat on Windows)")
    print("2. Start the frontend: ./start_frontend.sh (or start_frontend.bat on Windows)")
    print("3. Open http://localhost:8501 in your browser")
    print("\n📚 For more information, see README_v2.md")
    
    return True

if __name__ == "__main__":
    main()
