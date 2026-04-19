import json
import logging
from typing import Dict, List, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class ExecutionPlanner:
    def __init__(self):
        self.execution_history = []
        
    def create_execution_plan(self, intent: Dict[str, Any], query: str) -> Dict[str, Any]:
        """Create execution plan based on user intent and query"""
        
        plan = {
            "plan_id": f"plan_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "query": query,
            "intent": intent,
            "created_at": datetime.now().isoformat(),
            "execution_steps": [],
            "data_sources_required": intent.get("data_sources", []),
            "estimated_complexity": self._estimate_complexity(intent, query),
            "merge_strategy": self._determine_merge_strategy(intent)
        }
        
        # Create execution steps based on intent
        steps = self._generate_execution_steps(intent, query)
        plan["execution_steps"] = steps
        
        # Store in history
        self.execution_history.append(plan)
        
        return plan
    
    def _estimate_complexity(self, intent: Dict[str, Any], query: str) -> str:
        """Estimate query complexity"""
        complexity_score = 0
        
        # Base score for intent type
        intent_type = intent.get("intent", "search")
        if intent_type == "search":
            complexity_score += 1
        elif intent_type == "analysis":
            complexity_score += 3
        elif intent_type == "comparison":
            complexity_score += 4
        elif intent_type == "aggregation":
            complexity_score += 2
        
        # Score for data sources
        data_sources = intent.get("data_sources", [])
        complexity_score += len(data_sources)
        
        # Score for entities and relationships
        entities = intent.get("entities", [])
        relationships = intent.get("relationships", [])
        complexity_score += len(entities) + len(relationships) * 2
        
        # Score for query length and complexity
        if len(query) > 100:
            complexity_score += 1
        if any(keyword in query.lower() for keyword in ["join", "merge", "combine", "compare"]):
            complexity_score += 2
        
        # Determine complexity level
        if complexity_score <= 3:
            return "low"
        elif complexity_score <= 7:
            return "medium"
        else:
            return "high"
    
    def _determine_merge_strategy(self, intent: Dict[str, Any]) -> str:
        """Determine how to merge results from different sources"""
        intent_type = intent.get("intent", "search")
        data_sources = intent.get("data_sources", [])
        
        if len(data_sources) <= 1:
            return "none"
        elif intent_type == "comparison":
            return "side_by_side"
        elif intent_type == "aggregation":
            return "union"
        else:
            return "semantic"
    
    def _generate_execution_steps(self, intent: Dict[str, Any], query: str) -> List[Dict[str, Any]]:
        """Generate detailed execution steps"""
        steps = []
        step_id = 1
        
        # Step 1: Query preprocessing
        steps.append({
            "step_id": step_id,
            "step_name": "Query Preprocessing",
            "step_type": "preprocessing",
            "description": "Extract keywords, entities, and normalize query",
            "parameters": {
                "query": query,
                "entities": intent.get("entities", []),
                "relationships": intent.get("relationships", [])
            },
            "expected_output": "processed_query",
            "dependencies": []
        })
        step_id += 1
        
        # Step 2: Generate embeddings
        steps.append({
            "step_id": step_id,
            "step_name": "Generate Query Embeddings",
            "step_type": "embedding",
            "description": "Generate embeddings for semantic search",
            "parameters": {
                "text": query,
                "model": "nomic-embed-text"
            },
            "expected_output": "query_embeddings",
            "dependencies": [step_id - 1]
        })
        step_id += 1
        
        # Step 3: Search PageIndex (if documents are involved)
        data_sources = intent.get("data_sources", [])
        if "documents" in data_sources or "all" in data_sources:
            steps.append({
                "step_id": step_id,
                "step_name": "Search Document Index",
                "step_type": "document_search",
                "description": "Search indexed documents using PageIndex",
                "parameters": {
                    "embeddings": "query_embeddings",
                    "top_k": 5,
                    "threshold": 0.3
                },
                "expected_output": "document_results",
                "dependencies": [step_id - 1]
            })
            step_id += 1
        
        # Step 4: Query MySQL (if needed)
        if "mysql" in data_sources or "all" in data_sources:
            steps.append({
                "step_id": step_id,
                "step_name": "Query MySQL Database",
                "step_type": "database_query",
                "description": "Execute SQL query on MySQL database",
                "parameters": {
                    "query_type": intent.get("intent", "search"),
                    "entities": intent.get("entities", []),
                    "tables": "auto_detect"
                },
                "expected_output": "mysql_results",
                "dependencies": [1]  # Only needs processed query
            })
            step_id += 1
        
        # Step 5: Query MongoDB (if needed)
        if "mongodb" in data_sources or "all" in data_sources:
            steps.append({
                "step_id": step_id,
                "step_name": "Query MongoDB Collection",
                "step_type": "database_query",
                "description": "Execute MongoDB query",
                "parameters": {
                    "query_type": intent.get("intent", "search"),
                    "entities": intent.get("entities", []),
                    "collections": "auto_detect"
                },
                "expected_output": "mongodb_results",
                "dependencies": [1]  # Only needs processed query
            })
            step_id += 1
        
        # Step 6: Merge results (if multiple sources)
        if len([s for s in data_sources if s in ["mysql", "mongodb", "documents", "all"]]) > 1:
            steps.append({
                "step_id": step_id,
                "step_name": "Merge Results",
                "step_type": "merge",
                "description": f"Merge results from multiple sources using {self._determine_merge_strategy(intent)} strategy",
                "parameters": {
                    "strategy": self._determine_merge_strategy(intent),
                    "sources": data_sources
                },
                "expected_output": "merged_results",
                "dependencies": [s["step_id"] for s in steps if s["step_type"] in ["document_search", "database_query"]]
            })
            step_id += 1
        
        # Step 7: Generate final response
        steps.append({
            "step_id": step_id,
            "step_name": "Generate Final Response",
            "step_type": "response_generation",
            "description": "Generate comprehensive answer using LLM",
            "parameters": {
                "context": "merged_results" if step_id > 6 else "single_source_results",
                "query": query,
                "model": "llama3.2"
            },
            "expected_output": "final_response",
            "dependencies": [step_id - 1]
        })
        
        return steps
    
    def get_execution_plan(self, plan_id: str) -> Dict[str, Any]:
        """Retrieve execution plan by ID"""
        for plan in self.execution_history:
            if plan["plan_id"] == plan_id:
                return plan
        return {}
    
    def update_execution_status(self, plan_id: str, step_id: int, status: str, result: Any = None):
        """Update execution status of a step"""
        for plan in self.execution_history:
            if plan["plan_id"] == plan_id:
                for step in plan["execution_steps"]:
                    if step["step_id"] == step_id:
                        step["status"] = status
                        step["completed_at"] = datetime.now().isoformat()
                        if result:
                            step["result"] = result
                        break
                break
    
    def get_execution_summary(self, plan_id: str) -> Dict[str, Any]:
        """Get execution summary for a plan"""
        plan = self.get_execution_plan(plan_id)
        if not plan:
            return {}
        
        completed_steps = [s for s in plan["execution_steps"] if s.get("status") == "completed"]
        total_steps = len(plan["execution_steps"])
        
        return {
            "plan_id": plan_id,
            "query": plan["query"],
            "total_steps": total_steps,
            "completed_steps": len(completed_steps),
            "progress_percentage": (len(completed_steps) / total_steps) * 100 if total_steps > 0 else 0,
            "status": "completed" if len(completed_steps) == total_steps else "in_progress",
            "complexity": plan["estimated_complexity"],
            "data_sources": plan["data_sources_required"]
        }
