import logging
import json
import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)

class MergeLayer:
    def __init__(self):
        self.merge_history = []
        
    def merge_results(self, data_sources: Dict[str, Any], strategy: str = "semantic", 
                     query: str = "") -> Dict[str, Any]:
        """Merge results from multiple data sources using specified strategy"""
        
        merge_id = f"merge_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            if strategy == "none":
                merged_result = self._merge_none(data_sources)
            elif strategy == "side_by_side":
                merged_result = self._merge_side_by_side(data_sources)
            elif strategy == "union":
                merged_result = self._merge_union(data_sources)
            elif strategy == "semantic":
                merged_result = self._merge_semantic(data_sources, query)
            else:
                merged_result = self._merge_semantic(data_sources, query)  # Default to semantic
            
            # Add metadata
            merged_result["merge_id"] = merge_id
            merged_result["merge_strategy"] = strategy
            merged_result["sources_count"] = len(data_sources)
            merged_result["total_records"] = self._count_total_records(data_sources)
            merged_result["merge_timestamp"] = datetime.now().isoformat()
            
            # Store in history
            self.merge_history.append({
                "merge_id": merge_id,
                "strategy": strategy,
                "sources": list(data_sources.keys()),
                "timestamp": datetime.now().isoformat(),
                "success": True
            })
            
            return {
                "success": True,
                "merged_data": merged_result,
                "merge_id": merge_id
            }
            
        except Exception as e:
            logger.error(f"Merge error: {e}")
            
            # Store failed merge in history
            self.merge_history.append({
                "merge_id": merge_id,
                "strategy": strategy,
                "sources": list(data_sources.keys()),
                "timestamp": datetime.now().isoformat(),
                "success": False,
                "error": str(e)
            })
            
            return {
                "success": False,
                "error": str(e),
                "merge_id": merge_id
            }
    
    def _merge_none(self, data_sources: Dict[str, Any]) -> Dict[str, Any]:
        """No merging - return sources as-is"""
        return {
            "merge_type": "none",
            "sources": data_sources,
            "content": data_sources
        }
    
    def _merge_side_by_side(self, data_sources: Dict[str, Any]) -> Dict[str, Any]:
        """Merge results side by side for comparison"""
        merged_content = []
        
        for source_name, source_data in data_sources.items():
            if isinstance(source_data, dict) and "data" in source_data:
                content_item = {
                    "source": source_name,
                    "data": source_data["data"],
                    "record_count": self._count_records_in_source(source_data),
                    "summary": self._generate_source_summary(source_data)
                }
                merged_content.append(content_item)
        
        return {
            "merge_type": "side_by_side",
            "sources": list(data_sources.keys()),
            "content": merged_content,
            "comparison_ready": True
        }
    
    def _merge_union(self, data_sources: Dict[str, Any]) -> Dict[str, Any]:
        """Union all results into a single dataset"""
        all_records = []
        source_metadata = {}
        
        for source_name, source_data in data_sources.items():
            records = self._extract_records(source_data)
            source_metadata[source_name] = {
                "record_count": len(records),
                "fields": self._get_common_fields(records)
            }
            
            # Add source information to each record
            for record in records:
                record["_source"] = source_name
                all_records.append(record)
        
        # Try to create a unified DataFrame
        try:
            df = pd.DataFrame(all_records)
            
            return {
                "merge_type": "union",
                "total_records": len(all_records),
                "unified_dataframe": df.to_dict('records'),
                "columns": list(df.columns),
                "source_metadata": source_metadata,
                "data_types": df.dtypes.to_dict()
            }
        except Exception as e:
            logger.warning(f"Could not create unified DataFrame: {e}")
            
            return {
                "merge_type": "union",
                "total_records": len(all_records),
                "records": all_records,
                "source_metadata": source_metadata
            }
    
    def _merge_semantic(self, data_sources: Dict[str, Any], query: str) -> Dict[str, Any]:
        """Merge results using semantic similarity and relevance to query"""
        relevant_content = []
        relevance_scores = {}
        
        # Extract text content from all sources
        all_text_content = []
        source_mapping = []
        
        for source_name, source_data in data_sources.items():
            text_content = self._extract_text_content(source_data)
            
            for i, text in enumerate(text_content):
                all_text_content.append(text)
                source_mapping.append({
                    "source": source_name,
                    "content_index": i,
                    "original_data": source_data
                })
        
        if not all_text_content:
            return {
                "merge_type": "semantic",
                "content": [],
                "message": "No text content found for semantic merging"
            }
        
        # Calculate relevance scores using TF-IDF
        try:
            # Create TF-IDF vectorizer
            vectorizer = TfidfVectorizer(stop_words='english', max_features=1000)
            
            # Fit and transform all content plus query
            all_texts = all_text_content + [query]
            tfidf_matrix = vectorizer.fit_transform(all_texts)
            
            # Calculate similarity with query
            query_vector = tfidf_matrix[-1]  # Last item is the query
            content_vectors = tfidf_matrix[:-1]  # All other items
            
            similarities = cosine_similarity(query_vector, content_vectors).flatten()
            
            # Filter content based on relevance threshold
            relevance_threshold = 0.1
            relevant_indices = np.where(similarities > relevance_threshold)[0]
            
            # Sort by relevance
            sorted_indices = relevant_indices[np.argsort(similarities[relevant_indices])[::-1]]
            
            for idx in sorted_indices:
                source_info = source_mapping[idx]
                relevance_score = similarities[idx]
                
                relevant_content.append({
                    "source": source_info["source"],
                    "content": all_text_content[idx],
                    "relevance_score": float(relevance_score),
                    "original_data": source_info["original_data"]
                })
            
            # Group by source
            grouped_content = {}
            for item in relevant_content:
                source = item["source"]
                if source not in grouped_content:
                    grouped_content[source] = []
                grouped_content[source].append(item)
            
            return {
                "merge_type": "semantic",
                "query": query,
                "total_content_items": len(all_text_content),
                "relevant_items": len(relevant_content),
                "relevance_threshold": relevance_threshold,
                "grouped_by_source": grouped_content,
                "top_relevant_content": relevant_content[:10]  # Top 10 most relevant
            }
            
        except Exception as e:
            logger.warning(f"Semantic merging failed, falling back to simple merge: {e}")
            return self._merge_union(data_sources)
    
    def _extract_records(self, source_data: Any) -> List[Dict[str, Any]]:
        """Extract records from source data"""
        records = []
        
        if isinstance(source_data, dict):
            if "data" in source_data:
                data = source_data["data"]
                
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            if "data" in item and isinstance(item["data"], list):
                                records.extend(item["data"])
                            else:
                                records.append(item)
                elif isinstance(data, dict):
                    records.append(data)
        
        return records
    
    def _extract_text_content(self, source_data: Any) -> List[str]:
        """Extract text content from source data"""
        text_content = []
        
        records = self._extract_records(source_data)
        
        for record in records:
            if isinstance(record, dict):
                # Convert all values to strings and join
                text_parts = []
                for key, value in record.items():
                    if key != '_id':  # Skip MongoDB ObjectId
                        text_parts.append(f"{key}: {value}")
                
                if text_parts:
                    text_content.append(" | ".join(text_parts))
            elif isinstance(record, str):
                text_content.append(record)
        
        return text_content
    
    def _count_records_in_source(self, source_data: Any) -> int:
        """Count total records in a source"""
        return len(self._extract_records(source_data))
    
    def _count_total_records(self, data_sources: Dict[str, Any]) -> int:
        """Count total records across all sources"""
        total = 0
        for source_data in data_sources.values():
            total += self._count_records_in_source(source_data)
        return total
    
    def _get_common_fields(self, records: List[Dict[str, Any]]) -> List[str]:
        """Get common fields across records"""
        if not records:
            return []
        
        field_counts = {}
        for record in records:
            if isinstance(record, dict):
                for field in record.keys():
                    field_counts[field] = field_counts.get(field, 0) + 1
        
        # Return fields that appear in at least 50% of records
        threshold = len(records) * 0.5
        return [field for field, count in field_counts.items() if count >= threshold]
    
    def _generate_source_summary(self, source_data: Any) -> Dict[str, Any]:
        """Generate summary for a source"""
        records = self._extract_records(source_data)
        
        summary = {
            "record_count": len(records),
            "has_data": len(records) > 0
        }
        
        if records:
            # Get sample fields
            sample_record = records[0]
            if isinstance(sample_record, dict):
                summary["sample_fields"] = list(sample_record.keys())[:5]  # First 5 fields
                
                # Try to get data types
                field_types = {}
                for field, value in sample_record.items():
                    field_types[field] = type(value).__name__
                summary["field_types"] = field_types
        
        return summary
    
    def get_merge_history(self) -> List[Dict[str, Any]]:
        """Get history of merge operations"""
        return self.merge_history
    
    def get_merge_result(self, merge_id: str) -> Dict[str, Any]:
        """Get specific merge result by ID (placeholder - would need storage)"""
        # In production, this would retrieve from a database
        for merge in self.merge_history:
            if merge["merge_id"] == merge_id:
                return merge
        return {}
