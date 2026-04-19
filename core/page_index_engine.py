import json
import os
import sys
import numpy as np
from typing import List, Dict, Any, Tuple
import logging
from sklearn.metrics.pairwise import cosine_similarity

# Add PageIndex to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'PageIndex'))

logger = logging.getLogger(__name__)

class PageIndexEngine:
    def __init__(self, page_index_path: str = None):
        self.page_index_path = page_index_path or os.path.join(os.path.dirname(__file__), '..', 'PageIndex')
        self.indexed_documents = {}
        self.embeddings_cache = {}
        
    def index_document(self, file_path: str, doc_type: str = "pdf") -> Dict[str, Any]:
        """Index a document using PageIndex"""
        try:
            if doc_type.lower() == "pdf":
                import subprocess
                import tempfile
                
                # Create temporary output directory
                with tempfile.TemporaryDirectory() as temp_dir:
                    cmd = [
                        "python", 
                        os.path.join(self.page_index_path, "run_pageindex.py"),
                        "--pdf_path", file_path,
                        "--model", "llama3.2",
                        "--if-add-node-summary", "yes",
                        "--if-add-doc-description", "yes",
                        "--if-add-node-text", "yes"
                    ]
                    
                    result = subprocess.run(cmd, cwd=self.page_index_path, 
                                          capture_output=True, text=True, timeout=300)
                    
                    if result.returncode == 0:
                        # Read the generated structure
                        output_file = os.path.join(self.page_index_path, "results", 
                                                  f"{os.path.splitext(os.path.basename(file_path))[0]}_structure.json")
                        
                        if os.path.exists(output_file):
                            with open(output_file, 'r', encoding='utf-8') as f:
                                structure = json.load(f)
                            
                            doc_id = f"doc_{len(self.indexed_documents)}"
                            self.indexed_documents[doc_id] = {
                                "file_path": file_path,
                                "type": doc_type,
                                "structure": structure,
                                "indexed_at": str(np.datetime64('now'))
                            }
                            
                            return {"success": True, "doc_id": doc_id, "structure": structure}
                        else:
                            return {"success": False, "error": "Output file not generated"}
                    else:
                        return {"success": False, "error": result.stderr}
                        
            elif doc_type.lower() == "markdown":
                import subprocess
                import tempfile
                
                with tempfile.TemporaryDirectory() as temp_dir:
                    cmd = [
                        "python",
                        os.path.join(self.page_index_path, "run_pageindex.py"),
                        "--md_path", file_path,
                        "--model", "llama3.2",
                        "--if-add-node-summary", "yes",
                        "--if-add-doc-description", "yes",
                        "--if-add-node-text", "yes"
                    ]
                    
                    result = subprocess.run(cmd, cwd=self.page_index_path,
                                          capture_output=True, text=True, timeout=300)
                    
                    if result.returncode == 0:
                        output_file = os.path.join(self.page_index_path, "results",
                                                  f"{os.path.splitext(os.path.basename(file_path))[0]}_structure.json")
                        
                        if os.path.exists(output_file):
                            with open(output_file, 'r', encoding='utf-8') as f:
                                structure = json.load(f)
                            
                            doc_id = f"doc_{len(self.indexed_documents)}"
                            self.indexed_documents[doc_id] = {
                                "file_path": file_path,
                                "type": doc_type,
                                "structure": structure,
                                "indexed_at": str(np.datetime64('now'))
                            }
                            
                            return {"success": True, "doc_id": doc_id, "structure": structure}
                        else:
                            return {"success": False, "error": "Output file not generated"}
                    else:
                        return {"success": False, "error": result.stderr}
            
            else:
                return {"success": False, "error": f"Unsupported document type: {doc_type}"}
                
        except Exception as e:
            logger.error(f"Document indexing error: {e}")
            return {"success": False, "error": str(e)}
    
    def search_documents(self, query_embedding: List[float], top_k: int = 5) -> List[Dict[str, Any]]:
        """Search indexed documents using query embedding"""
        results = []
        
        for doc_id, doc_data in self.indexed_documents.items():
            structure = doc_data["structure"]
            
            # Extract text content from structure
            text_chunks = self._extract_text_from_structure(structure)
            
            for i, chunk in enumerate(text_chunks):
                # Generate embedding for chunk (simplified - in production, cache these)
                chunk_embedding = self._get_chunk_embedding(chunk)
                
                # Calculate similarity
                similarity = cosine_similarity([query_embedding], [chunk_embedding])[0][0]
                
                if similarity > 0.3:  # Threshold
                    results.append({
                        "doc_id": doc_id,
                        "chunk_id": f"{doc_id}_{i}",
                        "text": chunk,
                        "similarity": float(similarity),
                        "file_path": doc_data["file_path"]
                    })
        
        # Sort by similarity and return top_k
        results.sort(key=lambda x: x["similarity"], reverse=True)
        return results[:top_k]
    
    def _extract_text_from_structure(self, structure: Dict[str, Any]) -> List[str]:
        """Extract text content from PageIndex structure"""
        texts = []
        
        def extract_recursive(node):
            if isinstance(node, dict):
                if "text" in node and node["text"]:
                    texts.append(node["text"])
                if "summary" in node and node["summary"]:
                    texts.append(node["summary"])
                
                for key, value in node.items():
                    if isinstance(value, (dict, list)):
                        extract_recursive(value)
            elif isinstance(node, list):
                for item in node:
                    extract_recursive(item)
        
        extract_recursive(structure)
        return texts
    
    def _get_chunk_embedding(self, text: str) -> List[float]:
        """Get embedding for text chunk (simplified version)"""
        # In production, use proper embedding model
        # For now, return a simple hash-based embedding
        import hashlib
        hash_obj = hashlib.md5(text.encode())
        hash_hex = hash_obj.hexdigest()
        
        # Convert to numeric embedding
        embedding = []
        for i in range(0, len(hash_hex), 2):
            hex_pair = hash_hex[i:i+2]
            embedding.append(int(hex_pair, 16) / 255.0)
        
        # Pad or truncate to standard size
        while len(embedding) < 768:
            embedding.extend(embedding)
        return embedding[:768]
    
    def get_document_structure(self, doc_id: str) -> Dict[str, Any]:
        """Get the full structure of a document"""
        if doc_id in self.indexed_documents:
            return self.indexed_documents[doc_id]["structure"]
        return {}
    
    def list_indexed_documents(self) -> List[Dict[str, Any]]:
        """List all indexed documents"""
        return [
            {
                "doc_id": doc_id,
                "file_path": data["file_path"],
                "type": data["type"],
                "indexed_at": data["indexed_at"]
            }
            for doc_id, data in self.indexed_documents.items()
        ]
