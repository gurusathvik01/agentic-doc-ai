import logging
import json
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
import uuid

from .llm_engine import OllamaLLMEngine
from .page_index_engine import PageIndexEngine
from .execution_planner import ExecutionPlanner
from .query_engine import QueryEngine
from .merge_layer import MergeLayer
from .cache_manager import CacheManager
from .query_storage import QueryStorage

logger = logging.getLogger(__name__)

class AgenticOrchestrator:
    def __init__(self, mongo_uri: str = None, mysql_config: Dict = None):
        """Initialize the orchestrator with all components"""
        
        # Initialize all engines
        self.llm_engine = OllamaLLMEngine()
        self.page_index_engine = PageIndexEngine()
        self.execution_planner = ExecutionPlanner()
        self.query_engine = QueryEngine(mongo_uri, mysql_config)
        self.merge_layer = MergeLayer()
        
        # Initialize cache and storage
        self.cache_manager = CacheManager()
        self.query_storage = QueryStorage()
        
        # Query history (in-memory for current session)
        self.query_history = []
        
        logger.info("Agentic Orchestrator initialized successfully")
    
    async def process_query(self, query: str, user_id: str = None) -> Dict[str, Any]:
        """Main query processing pipeline following the architecture flow"""
        
        query_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        try:
            # Check cache first
            cached_result = self.cache_manager.get(query, {"user_id": user_id})
            if cached_result:
                logger.info(f"Cache hit for query: {query[:50]}...")
                cached_result["from_cache"] = True
                cached_result["cache_hit"] = True
                return cached_result
            
            # Step 1: User Query -> Ollama LLM (Intent)
            logger.info(f"Processing query {query_id}: {query}")
            
            intent_result = self._extract_intent(query)
            if not intent_result["success"]:
                return self._create_error_response(query_id, query, "Intent extraction failed", intent_result["error"])
            
            intent = intent_result["intent"]
            
            # Step 2: Ollama Embeddings
            embeddings_result = self._generate_embeddings(query)
            if not embeddings_result["success"]:
                return self._create_error_response(query_id, query, "Embedding generation failed", embeddings_result["error"])
            
            query_embeddings = embeddings_result["embeddings"]
            
            # Step 3: PageIndex (Search through indexed documents)
            page_index_result = await self._search_page_index(query_embeddings, intent)
            
            # Step 4: Execution Planner (JSON output)
            execution_plan = self.execution_planner.create_execution_plan(intent, query)
            
            # Step 5: Query Engine (Execute queries on MySQL, MongoDB, Docs)
            query_results = await self._execute_queries(execution_plan, query_embeddings)
            
            # Step 6: Merge Layer
            merge_result = self._merge_results(query_results, execution_plan, query)
            
            # Step 7: Ollama LLM (Final Answer)
            final_response = self._generate_final_response(merge_result, query, intent)
            
            # Create comprehensive response
            response = {
                "query_id": query_id,
                "query": query,
                "success": True,
                "timestamp": datetime.now().isoformat(),
                "processing_time": (datetime.now() - start_time).total_seconds(),
                "intent": intent,
                "execution_plan": execution_plan,
                "query_results": query_results,
                "merge_result": merge_result,
                "final_response": final_response,
                "sources_used": self._extract_sources_used(query_results),
                "data_summary": self._create_data_summary(query_results, merge_result)
            }
            
            # Store in cache
            self.cache_manager.set(query, response, {"user_id": user_id})
            
            # Store in persistent storage
            storage_data = {
                "query_id": query_id,
                "query": query,
                "user_id": user_id,
                "intent": intent,
                "execution_plan": execution_plan,
                "query_results": query_results,
                "merge_result": merge_result,
                "final_response": final_response,
                "sources_used": self._extract_sources_used(query_results),
                "data_summary": self._create_data_summary(query_results, merge_result),
                "processing_time": response["processing_time"],
                "timestamp": response["timestamp"],
                "status": "completed"
            }
            
            self.query_storage.store_query(storage_data)
            
            # Store analytics event
            self.query_storage.store_analytics_event(
                "query_processed",
                {
                    "query_id": query_id,
                    "processing_time": response["processing_time"],
                    "sources_used": response["sources_used"],
                    "success": True
                },
                user_id
            )
            
            # Store in memory history
            self.query_history.append(response)
            
            logger.info(f"Query {query_id} processed successfully in {response['processing_time']:.2f}s")
            return response
            
        except Exception as e:
            logger.error(f"Query processing failed for {query_id}: {e}")
            return self._create_error_response(query_id, query, "Processing failed", str(e))
    
    def _extract_intent(self, query: str) -> Dict[str, Any]:
        """Extract user intent using LLM"""
        try:
            intent = self.llm_engine.get_intent(query)
            return {"success": True, "intent": intent}
        except Exception as e:
            logger.error(f"Intent extraction error: {e}")
            return {"success": False, "error": str(e)}
    
    def _generate_embeddings(self, query: str) -> Dict[str, Any]:
        """Generate embeddings for the query"""
        try:
            embeddings = self.llm_engine.generate_embeddings([query])
            return {"success": True, "embeddings": embeddings[0] if embeddings else []}
        except Exception as e:
            logger.error(f"Embedding generation error: {e}")
            return {"success": False, "error": str(e)}
    
    async def _search_page_index(self, query_embeddings: List[float], intent: Dict[str, Any]) -> Dict[str, Any]:
        """Search through PageIndex for relevant documents"""
        try:
            data_sources = intent.get("data_sources", [])
            
            if "documents" in data_sources or "all" in data_sources:
                document_results = self.page_index_engine.search_documents(query_embeddings, top_k=5)
                return {"success": True, "documents": document_results}
            else:
                return {"success": True, "documents": []}
                
        except Exception as e:
            logger.error(f"PageIndex search error: {e}")
            return {"success": False, "error": str(e), "documents": []}
    
    async def _execute_queries(self, execution_plan: Dict[str, Any], query_embeddings: List[float]) -> Dict[str, Any]:
        """Execute queries based on execution plan"""
        query_results = {
            "mysql": {"success": False, "data": []},
            "mongodb": {"success": False, "data": []},
            "documents": {"success": False, "data": []}
        }
        
        # Execute each step in the plan
        for step in execution_plan["execution_steps"]:
            if step["step_type"] == "database_query":
                result = self.query_engine.execute_query(execution_plan, step)
                if result["success"]:
                    # Update query_results based on what was queried
                    data = result.get("data", {})
                    if "mysql" in data:
                        query_results["mysql"] = {"success": True, "data": data["mysql"]}
                    if "mongodb" in data:
                        query_results["mongodb"] = {"success": True, "data": data["mongodb"]}
            
            elif step["step_type"] == "document_search":
                # This would use the PageIndex results
                # For now, we'll simulate this
                query_results["documents"] = {"success": True, "data": []}
        
        return query_results
    
    def _merge_results(self, query_results: Dict[str, Any], execution_plan: Dict[str, Any], query: str) -> Dict[str, Any]:
        """Merge results from different sources"""
        try:
            # Filter successful results
            successful_sources = {k: v for k, v in query_results.items() if v["success"] and v["data"]}
            
            if not successful_sources:
                return {"success": False, "error": "No successful queries to merge"}
            
            # Get merge strategy from execution plan
            merge_strategy = execution_plan.get("merge_strategy", "semantic")
            
            # Perform merge
            merge_result = self.merge_layer.merge_results(successful_sources, merge_strategy, query)
            
            return merge_result
            
        except Exception as e:
            logger.error(f"Merge error: {e}")
            return {"success": False, "error": str(e)}
    
    def _generate_final_response(self, merge_result: Dict[str, Any], query: str, intent: Dict[str, Any]) -> str:
        """Generate final response using LLM"""
        try:
            if not merge_result["success"]:
                return f"I apologize, but I encountered an error while processing your query: {merge_result['error']}"
            
            # Create context from merged results
            context = self._create_context_from_merge(merge_result)
            
            # Generate response
            response = self.llm_engine.generate_response(context, query)
            
            return response
            
        except Exception as e:
            logger.error(f"Final response generation error: {e}")
            return f"I apologize, but I encountered an error while generating the response: {str(e)}"
    
    def _create_context_from_merge(self, merge_result: Dict[str, Any]) -> str:
        """Create context string from merge results for LLM"""
        if not merge_result["success"]:
            return "No data available to answer the query."
        
        merged_data = merge_result["merged_data"]
        context_parts = []
        
        if merged_data.get("merge_type") == "semantic":
            context_parts.append("Based on the search results:")
            
            if "grouped_by_source" in merged_data:
                for source, items in merged_data["grouped_by_source"].items():
                    context_parts.append(f"\nFrom {source}:")
                    for item in items[:3]:  # Top 3 items per source
                        context_parts.append(f"- {item['content'][:200]}...")
        
        elif merged_data.get("merge_type") == "union":
            context_parts.append(f"Found {merged_data.get('total_records', 0)} records from multiple sources.")
            
            if "unified_dataframe" in merged_data:
                # Show sample of unified data
                sample_records = merged_data["unified_dataframe"][:5]
                for record in sample_records:
                    context_parts.append(f"- {str(record)[:150]}...")
        
        elif merged_data.get("merge_type") == "side_by_side":
            context_parts.append("Comparison of data from different sources:")
            
            if "content" in merged_data:
                for source_info in merged_data["content"]:
                    source_name = source_info["source"]
                    record_count = source_info["record_count"]
                    context_parts.append(f"\n{source_name}: {record_count} records found")
        
        return "\n".join(context_parts)
    
    def _extract_sources_used(self, query_results: Dict[str, Any]) -> List[str]:
        """Extract list of sources that returned data"""
        sources_used = []
        
        for source, result in query_results.items():
            if result["success"] and result["data"]:
                sources_used.append(source)
        
        return sources_used
    
    def _create_data_summary(self, query_results: Dict[str, Any], merge_result: Dict[str, Any]) -> Dict[str, Any]:
        """Create summary of data processed"""
        summary = {
            "sources_queried": list(query_results.keys()),
            "successful_sources": self._extract_sources_used(query_results),
            "total_records": 0,
            "merge_strategy": merge_result.get("merged_data", {}).get("merge_strategy", "none"),
            "merge_successful": merge_result.get("success", False)
        }
        
        # Count total records
        for source, result in query_results.items():
            if result["success"] and isinstance(result["data"], list):
                summary["total_records"] += len(result["data"])
        
        return summary
    
    def _create_error_response(self, query_id: str, query: str, error_type: str, error_message: str) -> Dict[str, Any]:
        """Create standardized error response"""
        return {
            "query_id": query_id,
            "query": query,
            "success": False,
            "error_type": error_type,
            "error_message": error_message,
            "timestamp": datetime.now().isoformat(),
            "final_response": f"I apologize, but I encountered an {error_type.lower()}: {error_message}"
        }
    
    def index_document(self, file_path: str, doc_type: str = "pdf") -> Dict[str, Any]:
        """Index a document using PageIndex"""
        return self.page_index_engine.index_document(file_path, doc_type)
    
    def get_available_sources(self) -> Dict[str, Any]:
        """Get information about available data sources"""
        return self.query_engine.get_available_sources()
    
    def get_query_history(self, limit: int = 10, user_id: str = None) -> List[Dict[str, Any]]:
        """Get recent query history"""
        if user_id:
            return self.query_storage.get_query_history(limit, user_id)
        else:
            # Return in-memory history for current session
            return self.query_history[-limit:]
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get system status and health"""
        return {
            "llm_engine": "operational",
            "page_index_engine": "operational",
            "execution_planner": "operational",
            "query_engine": self.query_engine.get_available_sources(),
            "merge_layer": "operational",
            "total_queries_processed": len(self.query_history),
            "cache_stats": self.cache_manager.get_stats(),
            "storage_stats": {
                "database_path": self.query_storage.db_path,
                "total_queries_stored": "Available via analytics"
            }
        }
    
    def get_analytics(self, days: int = 30) -> Dict[str, Any]:
        """Get analytics data"""
        return self.query_storage.get_analytics(days)
    
    def clear_history(self):
        """Clear query history and cache"""
        self.query_history.clear()
        self.cache_manager.invalidate()
        logger.info("Query history and cache cleared")
    
    def cleanup_system(self, days_to_keep: int = 90):
        """Clean up old data and expired cache entries"""
        self.cache_manager.cleanup_expired()
        self.query_storage.cleanup_old_data(days_to_keep)
        logger.info(f"System cleanup completed (keeping {days_to_keep} days)")
    
    def shutdown(self):
        """Shutdown the orchestrator and close connections"""
        self.query_engine.close_connections()
        logger.info("Agentic Orchestrator shutdown complete")
