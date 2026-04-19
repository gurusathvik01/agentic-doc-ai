#!/usr/bin/env python3
"""
Agentic Doc AI v2 System Test

Comprehensive testing script for the production-ready system.
Tests all components with multiple datasets.
"""

import os
import sys
import json
import time
import requests
import pandas as pd
from datetime import datetime
import tempfile
import subprocess
from pathlib import Path

# Configuration
API_URL = "http://127.0.0.1:8000"
TEST_DATA_DIR = "test_data"

class SystemTester:
    def __init__(self):
        self.test_results = []
        self.api_url = API_URL
        
    def log_test(self, test_name: str, success: bool, message: str = "", details: dict = None):
        """Log test results"""
        result = {
            "test_name": test_name,
            "success": success,
            "message": message,
            "details": details or {},
            "timestamp": datetime.now().isoformat()
        }
        self.test_results.append(result)
        
        status = "✅" if success else "❌"
        print(f"{status} {test_name}: {message}")
        
        if details and not success:
            print(f"   Details: {json.dumps(details, indent=2)}")
    
    def check_api_health(self):
        """Check if API is running"""
        try:
            response = requests.get(f"{self.api_url}/health", timeout=5)
            if response.status_code == 200:
                self.log_test("API Health Check", True, "API is running")
                return True
            else:
                self.log_test("API Health Check", False, f"API returned status {response.status_code}")
                return False
        except Exception as e:
            self.log_test("API Health Check", False, f"Connection failed: {str(e)}")
            return False
    
    def test_system_status(self):
        """Test system status endpoint"""
        try:
            response = requests.get(f"{self.api_url}/system-status", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    status = data.get("status", {})
                    
                    # Check core components
                    llm_ok = status.get("llm_engine") == "operational"
                    planner_ok = status.get("execution_planner") == "operational"
                    merge_ok = status.get("merge_layer") == "operational"
                    
                    all_ok = llm_ok and planner_ok and merge_ok
                    
                    self.log_test(
                        "System Status", 
                        all_ok, 
                        f"Components operational: LLM={llm_ok}, Planner={planner_ok}, Merge={merge_ok}",
                        status
                    )
                    return all_ok
                else:
                    self.log_test("System Status", False, "API returned unsuccessful response")
                    return False
            else:
                self.log_test("System Status", False, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test("System Status", False, f"Request failed: {str(e)}")
            return False
    
    def test_data_sources(self):
        """Test data source connections"""
        try:
            response = requests.get(f"{self.api_url}/sources", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    sources = data.get("sources", {})
                    
                    mysql_available = sources.get("mysql", {}).get("available", False)
                    mongo_available = sources.get("mongodb", {}).get("available", False)
                    
                    mysql_tables = len(sources.get("mysql", {}).get("tables", []))
                    mongo_collections = len(sources.get("mongodb", {}).get("collections", []))
                    
                    self.log_test(
                        "Data Sources",
                        True,
                        f"MySQL: {mysql_available} ({mysql_tables} tables), MongoDB: {mongo_available} ({mongo_collections} collections)",
                        sources
                    )
                    return True
                else:
                    self.log_test("Data Sources", False, "API returned unsuccessful response")
                    return False
            else:
                self.log_test("Data Sources", False, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test("Data Sources", False, f"Request failed: {str(e)}")
            return False
    
    def create_test_data(self):
        """Create test data files"""
        os.makedirs(TEST_DATA_DIR, exist_ok=True)
        
        # Create test CSV
        csv_data = {
            "id": [1, 2, 3, 4, 5],
            "product": ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"],
            "category": ["Electronics", "Electronics", "Electronics", "Electronics", "Electronics"],
            "price": [999.99, 699.99, 299.99, 199.99, 79.99],
            "stock": [50, 100, 75, 30, 200],
            "rating": [4.5, 4.3, 4.1, 4.6, 4.2]
        }
        
        df = pd.DataFrame(csv_data)
        csv_path = os.path.join(TEST_DATA_DIR, "test_products.csv")
        df.to_csv(csv_path, index=False)
        
        # Create test markdown
        md_content = """# Product Documentation

## Laptop Specifications

The laptop is a high-performance device designed for professionals.

### Key Features
- Intel Core i7 processor
- 16GB RAM
- 512GB SSD
- 15.6" display

## Phone Specifications

The smartphone offers advanced features for modern users.

### Key Features
- Octa-core processor
- 8GB RAM
- 256GB storage
- 6.5" display

## Tablet Overview

Tablets provide portable computing solutions.

### Key Features
- Quad-core processor
- 4GB RAM
- 128GB storage
- 10.1" display
"""
        
        md_path = os.path.join(TEST_DATA_DIR, "test_docs.md")
        with open(md_path, "w") as f:
            f.write(md_content)
        
        self.log_test("Test Data Creation", True, f"Created test CSV and markdown files")
        return csv_path, md_path
    
    def test_document_indexing(self, md_path: str):
        """Test document indexing"""
        try:
            with open(md_path, 'rb') as f:
                files = {'file': ('test_docs.md', f, 'text/markdown')}
                response = requests.post(f"{self.api_url}/upload-and-index", files=files, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    self.log_test(
                        "Document Indexing",
                        True,
                        f"Document indexed successfully: {data.get('doc_id')}",
                        data
                    )
                    return data.get("doc_id")
                else:
                    self.log_test("Document Indexing", False, f"Indexing failed: {data.get('error')}")
                    return None
            else:
                self.log_test("Document Indexing", False, f"HTTP {response.status_code}")
                return None
                
        except Exception as e:
            self.log_test("Document Indexing", False, f"Request failed: {str(e)}")
            return None
    
    def test_query_processing(self):
        """Test various types of queries"""
        test_queries = [
            {
                "name": "Simple Search Query",
                "query": "What products are available?",
                "expected_sources": ["mysql", "mongodb"]
            },
            {
                "name": "Document Query",
                "query": "What are the laptop specifications?",
                "expected_sources": ["documents"]
            },
            {
                "name": "Multi-source Query",
                "query": "Show me electronics products and their documentation",
                "expected_sources": ["mysql", "mongodb", "documents"]
            },
            {
                "name": "Analysis Query",
                "query": "What is the average price of products?",
                "expected_sources": ["mysql", "mongodb"]
            }
        ]
        
        for test_case in test_queries:
            try:
                start_time = time.time()
                response = requests.post(
                    f"{self.api_url}/query",
                    json={"query": test_case["query"]},
                    timeout=120
                )
                processing_time = time.time() - start_time
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("success"):
                        sources_used = data.get("sources_used", [])
                        processing_time_api = data.get("processing_time", 0)
                        
                        # Check if expected sources were used
                        sources_match = any(source in sources_used for source in test_case["expected_sources"])
                        
                        self.log_test(
                            f"Query: {test_case['name']}",
                            sources_match,
                            f"Sources used: {sources_used}, Time: {processing_time_api:.2f}s",
                            {
                                "query": test_case["query"],
                                "sources_used": sources_used,
                                "expected_sources": test_case["expected_sources"],
                                "processing_time": processing_time_api,
                                "response_length": len(data.get("final_response", ""))
                            }
                        )
                    else:
                        self.log_test(
                            f"Query: {test_case['name']}",
                            False,
                            f"Query processing failed: {data.get('error_message', 'Unknown error')}"
                        )
                else:
                    self.log_test(
                        f"Query: {test_case['name']}",
                        False,
                        f"HTTP {response.status_code}"
                    )
                    
            except Exception as e:
                self.log_test(
                    f"Query: {test_case['name']}",
                    False,
                    f"Request failed: {str(e)}"
                )
    
    def test_report_generation(self):
        """Test JSON report generation"""
        try:
            # First, get a query from history
            response = requests.get(f"{self.api_url}/query-history?limit=1", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and data.get("history"):
                    query_id = data["history"][0].get("query_id")
                    
                    if query_id:
                        # Generate report
                        report_response = requests.get(f"{self.api_url}/report/{query_id}", timeout=15)
                        
                        if report_response.status_code == 200:
                            report_data = report_response.json()
                            if report_data.get("success"):
                                report = report_data.get("report", {})
                                
                                self.log_test(
                                    "Report Generation",
                                    True,
                                    f"Report generated for query {query_id}",
                                    {
                                        "report_id": report.get("report_id"),
                                        "has_query_info": "query_info" in report,
                                        "has_intent": "intent_analysis" in report,
                                        "has_execution_plan": "execution_plan" in report,
                                        "has_results": "results" in report,
                                        "has_response": "final_response" in report
                                    }
                                )
                                return True
                            else:
                                self.log_test("Report Generation", False, "Report generation failed")
                                return False
                        else:
                            self.log_test("Report Generation", False, f"HTTP {report_response.status_code}")
                            return False
                    else:
                        self.log_test("Report Generation", False, "No query ID found in history")
                        return False
                else:
                    self.log_test("Report Generation", False, "No query history available")
                    return False
            else:
                self.log_test("Report Generation", False, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test("Report Generation", False, f"Request failed: {str(e)}")
            return False
    
    def test_performance(self):
        """Test system performance"""
        print("\n🚀 Performance Testing...")
        
        # Test multiple concurrent queries
        queries = [
            "List all products",
            "What is the price of laptop?",
            "Show me tablet documentation",
            "Analyze product ratings"
        ]
        
        start_time = time.time()
        successful_queries = 0
        
        for i, query in enumerate(queries):
            try:
                response = requests.post(
                    f"{self.api_url}/query",
                    json={"query": query},
                    timeout=60
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("success"):
                        successful_queries += 1
                        
            except:
                pass
        
        total_time = time.time() - start_time
        avg_time = total_time / len(queries)
        
        self.log_test(
            "Performance Test",
            successful_queries >= len(queries) * 0.75,  # At least 75% success
            f"{successful_queries}/{len(queries)} queries successful, Avg time: {avg_time:.2f}s",
            {
                "total_queries": len(queries),
                "successful_queries": successful_queries,
                "total_time": total_time,
                "average_time": avg_time,
                "queries_per_second": len(queries) / total_time if total_time > 0 else 0
            }
        )
    
    def generate_report(self):
        """Generate comprehensive test report"""
        total_tests = len(self.test_results)
        successful_tests = sum(1 for result in self.test_results if result["success"])
        
        report = {
            "test_summary": {
                "total_tests": total_tests,
                "successful_tests": successful_tests,
                "failed_tests": total_tests - successful_tests,
                "success_rate": (successful_tests / total_tests) * 100 if total_tests > 0 else 0,
                "test_date": datetime.now().isoformat()
            },
            "test_results": self.test_results,
            "recommendations": self._generate_recommendations()
        }
        
        # Save report
        report_path = f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📊 Test Report saved to: {report_path}")
        
        # Print summary
        print(f"\n📈 Test Summary:")
        print(f"Total Tests: {total_tests}")
        print(f"Successful: {successful_tests}")
        print(f"Failed: {total_tests - successful_tests}")
        print(f"Success Rate: {report['test_summary']['success_rate']:.1f}%")
        
        return report_path
    
    def _generate_recommendations(self):
        """Generate recommendations based on test results"""
        recommendations = []
        
        # Check for failed tests
        failed_tests = [result for result in self.test_results if not result["success"]]
        
        for failed_test in failed_tests:
            test_name = failed_test["test_name"]
            
            if "API Health" in test_name:
                recommendations.append("Start the backend server: python backend/main_v2.py")
            elif "System Status" in test_name:
                recommendations.append("Check Ollama installation and models")
            elif "Data Sources" in test_name:
                recommendations.append("Verify database connections and configurations")
            elif "Document Indexing" in test_name:
                recommendations.append("Check PageIndex installation and dependencies")
            elif "Query" in test_name:
                recommendations.append("Verify LLM models are downloaded and accessible")
            elif "Report Generation" in test_name:
                recommendations.append("Ensure query history is available")
        
        if not failed_tests:
            recommendations.append("All tests passed! System is ready for production use.")
        
        return recommendations
    
    def run_all_tests(self):
        """Run all system tests"""
        print("🧪 Agentic Doc AI v2 System Testing")
        print("=" * 60)
        
        # Basic connectivity tests
        if not self.check_api_health():
            print("\n❌ API is not running. Please start the backend server first.")
            return False
        
        # System tests
        self.test_system_status()
        self.test_data_sources()
        
        # Create test data
        csv_path, md_path = self.create_test_data()
        
        # Document indexing test
        self.test_document_indexing(md_path)
        
        # Query processing tests
        self.test_query_processing()
        
        # Report generation test
        self.test_report_generation()
        
        # Performance tests
        self.test_performance()
        
        # Generate report
        report_path = self.generate_report()
        
        return report_path

def main():
    """Main test function"""
    tester = SystemTester()
    
    try:
        report_path = tester.run_all_tests()
        
        if report_path:
            print(f"\n✅ Testing completed. Report saved to: {report_path}")
        else:
            print("\n❌ Testing failed. Check the output above for details.")
            
    except KeyboardInterrupt:
        print("\n⚠️ Testing interrupted by user")
    except Exception as e:
        print(f"\n❌ Testing failed with error: {str(e)}")

if __name__ == "__main__":
    main()
