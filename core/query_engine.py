import logging
import json
import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime
import pymongo
import mysql.connector
from mysql.connector import Error

logger = logging.getLogger(__name__)

class QueryEngine:
    def __init__(self, mongo_uri: str = None, mysql_config: Dict = None):
        self.mongo_client = None
        self.mysql_connection = None
        self.mysql_config = mysql_config or {
            'host': 'localhost',
            'user': 'root',
            'password': 'Gurusathvik@99',
            'database': 'agentic_ai'
        }
        
        # Initialize connections
        if mongo_uri:
            self._init_mongodb(mongo_uri)
        self._init_mysql()
    
    def _init_mongodb(self, uri: str):
        """Initialize MongoDB connection"""
        try:
            self.mongo_client = pymongo.MongoClient(uri)
            # Test connection
            self.mongo_client.admin.command('ping')
            logger.info("MongoDB connection established")
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            self.mongo_client = None
    
    def _init_mysql(self):
        """Initialize MySQL connection"""
        try:
            self.mysql_connection = mysql.connector.connect(**self.mysql_config)
            if self.mysql_connection.is_connected():
                logger.info("MySQL connection established")
        except Error as e:
            logger.error(f"MySQL connection failed: {e}")
            self.mysql_connection = None
    
    def execute_query(self, execution_plan: Dict[str, Any], step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a specific query step"""
        step_type = step["step_type"]
        step_name = step["step_name"]
        
        try:
            if step_type == "database_query":
                return self._execute_database_query(step)
            elif step_type == "document_search":
                return self._execute_document_search(step)
            elif step_type == "merge":
                return self._execute_merge(step)
            else:
                return {"success": False, "error": f"Unknown step type: {step_type}"}
                
        except Exception as e:
            logger.error(f"Query execution error for {step_name}: {e}")
            return {"success": False, "error": str(e)}
    
    def _execute_database_query(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute database query based on step parameters"""
        parameters = step["parameters"]
        query_type = parameters.get("query_type", "search")
        entities = parameters.get("entities", [])
        
        results = {}
        
        # Check if MySQL is needed and available
        if self.mysql_connection and ("mysql" in str(parameters) or "auto_detect" in str(parameters)):
            mysql_results = self._query_mysql(query_type, entities)
            if mysql_results["success"]:
                results["mysql"] = mysql_results["data"]
        
        # Check if MongoDB is needed and available
        if self.mongo_client and ("mongodb" in str(parameters) or "auto_detect" in str(parameters)):
            mongodb_results = self._query_mongodb(query_type, entities)
            if mongodb_results["success"]:
                results["mongodb"] = mongodb_results["data"]
        
        return {
            "success": True,
            "data": results,
            "step_name": step["step_name"],
            "executed_at": datetime.now().isoformat()
        }
    
    def _query_mysql(self, query_type: str, entities: List[str]) -> Dict[str, Any]:
        """Query MySQL database"""
        try:
            cursor = self.mysql_connection.cursor(dictionary=True)
            
            # Get available tables
            cursor.execute("SHOW TABLES")
            tables = [list(table.values())[0] for table in cursor.fetchall()]
            
            results = []
            
            for table in tables:
                try:
                    # Build query based on entities and query type
                    if entities:
                        # Search for entities in table columns
                        cursor.execute(f"DESCRIBE {table}")
                        columns = [col['Field'] for col in cursor.fetchall()]
                        
                        # Find columns that might contain entities
                        relevant_columns = [col for col in columns if any(entity.lower() in col.lower() for entity in entities)]
                        
                        if relevant_columns:
                            # Build WHERE clause
                            where_conditions = []
                            for entity in entities:
                                for col in relevant_columns:
                                    where_conditions.append(f"{col} LIKE '%{entity}%'")
                            
                            where_clause = " OR ".join(where_conditions)
                            query = f"SELECT * FROM {table} WHERE {where_clause} LIMIT 100"
                        else:
                            query = f"SELECT * FROM {table} LIMIT 50"
                    else:
                        query = f"SELECT * FROM {table} LIMIT 50"
                    
                    cursor.execute(query)
                    table_results = cursor.fetchall()
                    
                    if table_results:
                        results.append({
                            "table": table,
                            "data": table_results,
                            "row_count": len(table_results)
                        })
                        
                except Exception as e:
                    logger.warning(f"Error querying table {table}: {e}")
                    continue
            
            cursor.close()
            
            return {
                "success": True,
                "data": results,
                "total_tables_queried": len(tables),
                "tables_with_results": len(results)
            }
            
        except Exception as e:
            logger.error(f"MySQL query error: {e}")
            return {"success": False, "error": str(e)}
    
    def _query_mongodb(self, query_type: str, entities: List[str]) -> Dict[str, Any]:
        """Query MongoDB collections"""
        try:
            db = self.mongo_client.agentic_ai
            collections = db.list_collection_names()
            
            results = []
            
            for collection_name in collections:
                try:
                    collection = db[collection_name]
                    
                    # Build query based on entities
                    if entities:
                        query_conditions = []
                        for entity in entities:
                            query_conditions.append({
                                "$or": [
                                    {k: {"$regex": entity, "$options": "i"}} 
                                    for k in collection.find_one().keys() if k != '_id'
                                ]
                            })
                        
                        if query_conditions:
                            mongo_query = {"$or": query_conditions}
                        else:
                            mongo_query = {}
                    else:
                        mongo_query = {}
                    
                    # Execute query with limit
                    cursor = collection.find(mongo_query).limit(100)
                    collection_results = list(cursor)
                    
                    if collection_results:
                        # Convert ObjectId to string for JSON serialization
                        for doc in collection_results:
                            if '_id' in doc:
                                doc['_id'] = str(doc['_id'])
                        
                        results.append({
                            "collection": collection_name,
                            "data": collection_results,
                            "document_count": len(collection_results)
                        })
                        
                except Exception as e:
                    logger.warning(f"Error querying collection {collection_name}: {e}")
                    continue
            
            return {
                "success": True,
                "data": results,
                "total_collections_queried": len(collections),
                "collections_with_results": len(results)
            }
            
        except Exception as e:
            logger.error(f"MongoDB query error: {e}")
            return {"success": False, "error": str(e)}
    
    def _execute_document_search(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Execute document search (placeholder - will be integrated with PageIndexEngine)"""
        # This will be called by the orchestrator with PageIndexEngine
        return {
            "success": True,
            "data": {"document_results": []},
            "step_name": step["step_name"],
            "executed_at": datetime.now().isoformat()
        }
    
    def _execute_merge(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Merge results from multiple sources"""
        parameters = step["parameters"]
        strategy = parameters.get("strategy", "semantic")
        sources = parameters.get("sources", [])
        
        # This will be called by the orchestrator with actual results
        return {
            "success": True,
            "data": {
                "merge_strategy": strategy,
                "sources_merged": sources,
                "merged_data": {}
            },
            "step_name": step["step_name"],
            "executed_at": datetime.now().isoformat()
        }
    
    def merge_data_sources(self, data_sources: Dict[str, Any], strategy: str = "semantic") -> Dict[str, Any]:
        """Merge data from multiple sources"""
        merged_data = {
            "sources": list(data_sources.keys()),
            "merge_strategy": strategy,
            "merged_content": [],
            "metadata": {
                "total_records": 0,
                "source_counts": {}
            }
        }
        
        total_records = 0
        
        for source_name, source_data in data_sources.items():
            if isinstance(source_data, dict) and "data" in source_data:
                # Handle database results
                if isinstance(source_data["data"], list):
                    for item in source_data["data"]:
                        if isinstance(item, dict) and "table" in item:
                            # MySQL table results
                            for record in item["data"]:
                                merged_data["merged_content"].append({
                                    "source": source_name,
                                    "table": item["table"],
                                    "data": record
                                })
                                total_records += 1
                        elif isinstance(item, dict) and "collection" in item:
                            # MongoDB collection results
                            for record in item["data"]:
                                merged_data["merged_content"].append({
                                    "source": source_name,
                                    "collection": item["collection"],
                                    "data": record
                                })
                                total_records += 1
                else:
                    # Direct data
                    for record in source_data["data"]:
                        merged_data["merged_content"].append({
                            "source": source_name,
                            "data": record
                        })
                        total_records += 1
                
                merged_data["metadata"]["source_counts"][source_name] = len(source_data["data"])
        
        merged_data["metadata"]["total_records"] = total_records
        
        return merged_data
    
    def get_available_sources(self) -> Dict[str, Any]:
        """Get information about available data sources"""
        sources = {
            "mysql": {
                "available": self.mysql_connection is not None and self.mysql_connection.is_connected(),
                "tables": []
            },
            "mongodb": {
                "available": self.mongo_client is not None,
                "collections": []
            }
        }
        
        # Get MySQL tables
        if sources["mysql"]["available"]:
            try:
                cursor = self.mysql_connection.cursor()
                cursor.execute("SHOW TABLES")
                sources["mysql"]["tables"] = [list(table)[0] for table in cursor.fetchall()]
                cursor.close()
            except Exception as e:
                logger.error(f"Error getting MySQL tables: {e}")
        
        # Get MongoDB collections
        if sources["mongodb"]["available"]:
            try:
                db = self.mongo_client.agentic_ai
                sources["mongodb"]["collections"] = db.list_collection_names()
            except Exception as e:
                logger.error(f"Error getting MongoDB collections: {e}")
        
        return sources
    
    def close_connections(self):
        """Close database connections"""
        if self.mysql_connection and self.mysql_connection.is_connected():
            self.mysql_connection.close()
            logger.info("MySQL connection closed")
        
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")
