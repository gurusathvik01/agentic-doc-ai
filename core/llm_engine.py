import ollama
import json
import numpy as np
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class OllamaLLMEngine:
    def __init__(self, model_name: str = "llama3.2"):
        self.model_name = model_name
        self.embed_model = "nomic-embed-text"
        
    def get_intent(self, query: str) -> Dict[str, Any]:
        """Extract user intent and required data sources"""
        prompt = f"""
        Analyze this query and determine:
        1. User intent (search, analysis, comparison, aggregation)
        2. Required data sources (mongodb, mysql, documents, all)
        3. Key entities and relationships needed
        4. Expected output format
        
        Query: "{query}"
        
        Return JSON format:
        {{
            "intent": "string",
            "data_sources": ["string"],
            "entities": ["string"],
            "relationships": ["string"],
            "output_format": "string"
        }}
        """
        
        try:
            response = ollama.chat(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Extract JSON from response
            content = response['message']['content']
            # Find JSON in the response
            start_idx = content.find('{')
            end_idx = content.rfind('}') + 1
            
            if start_idx != -1 and end_idx != -1:
                json_str = content[start_idx:end_idx]
                return json.loads(json_str)
            else:
                # Fallback response
                return {
                    "intent": "search",
                    "data_sources": ["all"],
                    "entities": [],
                    "relationships": [],
                    "output_format": "text"
                }
                
        except Exception as e:
            logger.error(f"Intent extraction error: {e}")
            return {
                "intent": "search",
                "data_sources": ["all"],
                "entities": [],
                "relationships": [],
                "output_format": "text"
            }
    
    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for given texts"""
        embeddings = []
        
        for text in texts:
            try:
                response = ollama.embeddings(
                    model=self.embed_model,
                    prompt=text
                )
                embeddings.append(response['embedding'])
            except Exception as e:
                logger.error(f"Embedding error for text '{text[:50]}...': {e}")
                # Fallback to zero embedding
                embeddings.append([0.0] * 768)  # Default embedding size
                
        return embeddings
    
    def generate_response(self, context: str, query: str) -> str:
        """Generate final response based on context and query"""
        prompt = f"""
        Based on the following data and context, answer the user's query comprehensively.
        
        Context:
        {context}
        
        Query: {query}
        
        Provide a detailed, accurate response based on the available data.
        """
        
        try:
            response = ollama.chat(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}]
            )
            return response['message']['content']
        except Exception as e:
            logger.error(f"Response generation error: {e}")
            return f"Error generating response: {str(e)}"
