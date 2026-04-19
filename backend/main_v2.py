from fastapi import FastAPI, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
import logging
import os
import json
import uvicorn
from datetime import datetime

# Import our new core orchestrator
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from core.orchestrator import AgenticOrchestrator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Agentic Doc AI v2",
    description="Production-ready AI system for multi-dataset querying with PageIndex integration",
    version="2.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize orchestrator
orchestrator = AgenticOrchestrator(
    mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017/"),
    mysql_config={
        'host': os.getenv("MYSQL_HOST", "localhost"),
        'user': os.getenv("MYSQL_USER", "root"),
        'password': os.getenv("MYSQL_PASSWORD"),
        'database': os.getenv("MYSQL_DB", "agentic_ai")
    }
)

# Pydantic models
class QueryRequest(BaseModel):
    query: str
    user_id: Optional[str] = None

class DocumentIndexRequest(BaseModel):
    file_path: str
    doc_type: str = "pdf"

class QueryResponse(BaseModel):
    query_id: str
    query: str
    success: bool
    timestamp: str
    processing_time: float
    final_response: str
    sources_used: List[str]
    data_summary: Dict[str, Any]
    intent: Optional[Dict[str, Any]] = None
    execution_plan: Optional[Dict[str, Any]] = None

# ==============================
# 🚀 MAIN ENDPOINTS
# ==============================

@app.post("/query", response_model=QueryResponse)
async def process_query(request: QueryRequest):
    """Main query processing endpoint following the architecture flow"""
    try:
        logger.info(f"Processing query: {request.query}")
        
        # Process query through orchestrator
        result = await orchestrator.process_query(request.query)
        
        if result["success"]:
            return QueryResponse(
                query_id=result["query_id"],
                query=result["query"],
                success=result["success"],
                timestamp=result["timestamp"],
                processing_time=result["processing_time"],
                final_response=result["final_response"],
                sources_used=result["sources_used"],
                data_summary=result["data_summary"],
                intent=result.get("intent"),
                execution_plan=result.get("execution_plan")
            )
        else:
            raise HTTPException(status_code=500, detail=result.get("error_message", "Query processing failed"))
            
    except Exception as e:
        logger.error(f"Query processing error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/index-document")
async def index_document(request: DocumentIndexRequest):
    """Index a document using PageIndex"""
    try:
        result = orchestrator.index_document(request.file_path, request.doc_type)
        
        if result["success"]:
            return {
                "success": True,
                "doc_id": result["doc_id"],
                "message": "Document indexed successfully",
                "structure_preview": {
                    "doc_id": result["doc_id"],
                    "file_path": request.file_path,
                    "type": request.doc_type
                }
            }
        else:
            raise HTTPException(status_code=500, detail=result.get("error", "Document indexing failed"))
            
    except Exception as e:
        logger.error(f"Document indexing error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload-and-index")
async def upload_and_index_document(file: UploadFile = File(...)):
    """Upload and index a document in one step"""
    try:
        # Save uploaded file
        upload_dir = "uploads"
        os.makedirs(upload_dir, exist_ok=True)
        
        file_path = os.path.join(upload_dir, file.filename)
        
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Determine document type
        doc_type = "pdf" if file.filename.lower().endswith('.pdf') else "markdown"
        
        # Index the document
        result = orchestrator.index_document(file_path, doc_type)
        
        if result["success"]:
            return {
                "success": True,
                "filename": file.filename,
                "doc_id": result["doc_id"],
                "file_path": file_path,
                "doc_type": doc_type,
                "message": "Document uploaded and indexed successfully"
            }
        else:
            # Clean up uploaded file on failure
            if os.path.exists(file_path):
                os.remove(file_path)
            raise HTTPException(status_code=500, detail=result.get("error", "Document indexing failed"))
            
    except Exception as e:
        logger.error(f"Upload and index error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==============================
# 📊 DATA SOURCE ENDPOINTS
# ==============================

@app.get("/sources")
async def get_available_sources():
    """Get information about available data sources"""
    try:
        sources = orchestrator.get_available_sources()
        return {
            "success": True,
            "sources": sources,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Get sources error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/indexed-documents")
async def get_indexed_documents():
    """Get list of indexed documents"""
    try:
        documents = orchestrator.page_index_engine.list_indexed_documents()
        return {
            "success": True,
            "documents": documents,
            "count": len(documents),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Get indexed documents error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==============================
# 📈 ANALYTICS ENDPOINTS
# ==============================

@app.get("/query-history")
async def get_query_history(limit: int = 10):
    """Get recent query history"""
    try:
        history = orchestrator.get_query_history(limit)
        return {
            "success": True,
            "history": history,
            "count": len(history),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Get query history error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/system-status")
async def get_system_status():
    """Get system status and health"""
    try:
        status = orchestrator.get_system_status()
        return {
            "success": True,
            "status": status,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Get system status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==============================
# 📄 REPORT ENDPOINTS
# ==============================

@app.get("/report/{query_id}")
async def generate_report(query_id: str):
    """Generate JSON report for a specific query"""
    try:
        history = orchestrator.get_query_history(100)  # Get more history to find the query
        
        query_result = None
        for query in history:
            if query["query_id"] == query_id:
                query_result = query
                break
        
        if not query_result:
            raise HTTPException(status_code=404, detail="Query not found")
        
        # Generate comprehensive report
        report = {
            "report_id": f"report_{query_id}",
            "query_id": query_id,
            "generated_at": datetime.now().isoformat(),
            "query_info": {
                "query": query_result["query"],
                "timestamp": query_result["timestamp"],
                "processing_time": query_result["processing_time"]
            },
            "intent_analysis": query_result.get("intent", {}),
            "execution_plan": query_result.get("execution_plan", {}),
            "data_sources": {
                "sources_used": query_result["sources_used"],
                "summary": query_result["data_summary"]
            },
            "results": {
                "query_results": query_result.get("query_results", {}),
                "merge_result": query_result.get("merge_result", {})
            },
            "final_response": query_result["final_response"],
            "performance_metrics": {
                "processing_time_seconds": query_result["processing_time"],
                "sources_queried": len(query_result["sources_used"]),
                "data_points_processed": query_result["data_summary"].get("total_records", 0)
            }
        }
        
        return {
            "success": True,
            "report": report
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Generate report error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==============================
# 🏠 UTILITY ENDPOINTS
# ==============================

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "🚀 Agentic Doc AI v2 Running",
        "version": "2.0.0",
        "description": "Production-ready AI system for multi-dataset querying",
        "endpoints": {
            "query": "/query",
            "upload_index": "/upload-and-index",
            "sources": "/sources",
            "indexed_documents": "/indexed-documents",
            "query_history": "/query-history",
            "system_status": "/system-status",
            "report": "/report/{query_id}",
            "docs": "/docs"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "2.0.0"
    }

@app.post("/reset-system")
async def reset_system():
    """Reset system (clear history and cache)"""
    try:
        orchestrator.clear_history()
        return {
            "success": True,
            "message": "System reset successfully",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"System reset error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==============================
# 🚀 STARTUP
# ==============================

@app.on_event("startup")
async def startup_event():
    """Initialize system on startup"""
    logger.info("🚀 Agentic Doc AI v2 starting up...")
    logger.info("System initialized successfully")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("🔄 Shutting down Agentic Doc AI v2...")
    orchestrator.shutdown()
    logger.info("System shutdown complete")

if __name__ == "__main__":
    uvicorn.run(
        "main_v2:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
