import json
import os
import sqlite3
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import threading
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class QueryStorage:
    """Persistent storage for queries, results, and analytics"""
    
    def __init__(self, db_path: str = "data/queries.db"):
        self.db_path = db_path
        self.db_lock = threading.RLock()
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Initialize database
        self._init_database()
        
        logger.info(f"QueryStorage initialized with database: {db_path}")
    
    def _init_database(self):
        """Initialize SQLite database with required tables"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Queries table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS queries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query_id TEXT UNIQUE NOT NULL,
                    query TEXT NOT NULL,
                    user_id TEXT,
                    intent_data TEXT,
                    execution_plan TEXT,
                    sources_used TEXT,
                    processing_time REAL,
                    status TEXT DEFAULT 'completed',
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Query results table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS query_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query_id TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_name TEXT,
                    result_data TEXT,
                    record_count INTEGER DEFAULT 0,
                    success BOOLEAN DEFAULT 1,
                    error_message TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (query_id) REFERENCES queries (query_id)
                )
            ''')
            
            # Query responses table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS query_responses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query_id TEXT UNIQUE NOT NULL,
                    final_response TEXT NOT NULL,
                    merge_strategy TEXT,
                    data_summary TEXT,
                    metadata TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (query_id) REFERENCES queries (query_id)
                )
            ''')
            
            # User sessions table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT UNIQUE NOT NULL,
                    user_id TEXT,
                    start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    last_activity DATETIME DEFAULT CURRENT_TIMESTAMP,
                    query_count INTEGER DEFAULT 0,
                    metadata TEXT
                )
            ''')
            
            # Analytics table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS analytics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    event_data TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    user_id TEXT,
                    session_id TEXT
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_queries_timestamp ON queries(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_queries_user_id ON queries(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_query_results_query_id ON query_results(query_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_analytics_timestamp ON analytics(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_analytics_event_type ON analytics(event_type)')
            
            conn.commit()
    
    @contextmanager
    def _get_connection(self):
        """Get database connection with proper locking"""
        with self.db_lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            try:
                yield conn
            finally:
                conn.close()
    
    def store_query(self, query_data: Dict[str, Any]) -> bool:
        """Store a complete query with all its data"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Store main query
                cursor.execute('''
                    INSERT OR REPLACE INTO queries 
                    (query_id, query, user_id, intent_data, execution_plan, sources_used, 
                     processing_time, status, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    query_data.get("query_id"),
                    query_data.get("query"),
                    query_data.get("user_id"),
                    json.dumps(query_data.get("intent", {})),
                    json.dumps(query_data.get("execution_plan", {})),
                    json.dumps(query_data.get("sources_used", [])),
                    query_data.get("processing_time"),
                    query_data.get("status", "completed"),
                    query_data.get("timestamp") or datetime.now().isoformat()
                ))
                
                # Store query results
                query_results = query_data.get("query_results", {})
                if query_results:
                    for source_type, result_data in query_results.items():
                        if isinstance(result_data, dict):
                            cursor.execute('''
                                INSERT INTO query_results 
                                (query_id, source_type, source_name, result_data, record_count, success, error_message)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                query_data.get("query_id"),
                                source_type,
                                source_type,  # Use source_type as source_name for now
                                json.dumps(result_data),
                                result_data.get("data", []).__len__() if isinstance(result_data.get("data"), list) else 0,
                                result_data.get("success", True),
                                result_data.get("error")
                            ))
                
                # Store final response
                if "final_response" in query_data:
                    cursor.execute('''
                        INSERT OR REPLACE INTO query_responses 
                        (query_id, final_response, merge_strategy, data_summary, metadata)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        query_data.get("query_id"),
                        query_data.get("final_response"),
                        query_data.get("merge_result", {}).get("merged_data", {}).get("merge_strategy"),
                        json.dumps(query_data.get("data_summary", {})),
                        json.dumps(query_data.get("metadata", {}))
                    ))
                
                conn.commit()
                logger.debug(f"Stored query: {query_data.get('query_id')}")
                return True
                
        except Exception as e:
            logger.error(f"Error storing query: {e}")
            return False
    
    def get_query(self, query_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a complete query by ID"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Get main query
                cursor.execute('SELECT * FROM queries WHERE query_id = ?', (query_id,))
                query_row = cursor.fetchone()
                
                if not query_row:
                    return None
                
                query_data = dict(query_row)
                
                # Parse JSON fields
                if query_data["intent_data"]:
                    query_data["intent_data"] = json.loads(query_data["intent_data"])
                if query_data["execution_plan"]:
                    query_data["execution_plan"] = json.loads(query_data["execution_plan"])
                if query_data["sources_used"]:
                    query_data["sources_used"] = json.loads(query_data["sources_used"])
                
                # Get query results
                cursor.execute('SELECT * FROM query_results WHERE query_id = ?', (query_id,))
                results_rows = cursor.fetchall()
                
                query_results = {}
                for row in results_rows:
                    row_dict = dict(row)
                    if row_dict["result_data"]:
                        row_dict["result_data"] = json.loads(row_dict["result_data"])
                    query_results[row_dict["source_type"]] = row_dict
                
                query_data["query_results"] = query_results
                
                # Get response
                cursor.execute('SELECT * FROM query_responses WHERE query_id = ?', (query_id,))
                response_row = cursor.fetchone()
                
                if response_row:
                    response_data = dict(response_row)
                    if response_data["data_summary"]:
                        response_data["data_summary"] = json.loads(response_data["data_summary"])
                    if response_data["metadata"]:
                        response_data["metadata"] = json.loads(response_data["metadata"])
                    query_data["response"] = response_data
                
                return query_data
                
        except Exception as e:
            logger.error(f"Error retrieving query: {e}")
            return None
    
    def get_query_history(self, limit: int = 50, user_id: str = None) -> List[Dict[str, Any]]:
        """Get query history with optional user filtering"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                if user_id:
                    cursor.execute('''
                        SELECT query_id, query, user_id, processing_time, sources_used, 
                               status, timestamp 
                        FROM queries 
                        WHERE user_id = ? 
                        ORDER BY timestamp DESC 
                        LIMIT ?
                    ''', (user_id, limit))
                else:
                    cursor.execute('''
                        SELECT query_id, query, user_id, processing_time, sources_used, 
                               status, timestamp 
                        FROM queries 
                        ORDER BY timestamp DESC 
                        LIMIT ?
                    ''', (limit,))
                
                rows = cursor.fetchall()
                history = []
                
                for row in rows:
                    query_data = dict(row)
                    if query_data["sources_used"]:
                        query_data["sources_used"] = json.loads(query_data["sources_used"])
                    history.append(query_data)
                
                return history
                
        except Exception as e:
            logger.error(f"Error getting query history: {e}")
            return []
    
    def get_analytics(self, days: int = 30) -> Dict[str, Any]:
        """Get analytics data for the specified period"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Query statistics
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total_queries,
                        AVG(processing_time) as avg_processing_time,
                        COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_queries,
                        COUNT(DISTINCT user_id) as unique_users
                    FROM queries 
                    WHERE timestamp >= datetime('now', '-{} days')
                '''.format(days))
                
                stats = dict(cursor.fetchone())
                
                # Source usage statistics
                cursor.execute('''
                    SELECT 
                        source_type,
                        COUNT(*) as usage_count,
                        COUNT(CASE WHEN success = 1 THEN 1 END) as successful_count
                    FROM query_results qr
                    JOIN queries q ON qr.query_id = q.query_id
                    WHERE q.timestamp >= datetime('now', '-{} days')
                    GROUP BY source_type
                    ORDER BY usage_count DESC
                '''.format(days))
                
                source_stats = [dict(row) for row in cursor.fetchall()]
                
                # Daily query volume
                cursor.execute('''
                    SELECT 
                        DATE(timestamp) as date,
                        COUNT(*) as query_count
                    FROM queries 
                    WHERE timestamp >= datetime('now', '-{} days')
                    GROUP BY DATE(timestamp)
                    ORDER BY date DESC
                '''.format(days))
                
                daily_volume = [dict(row) for row in cursor.fetchall()]
                
                # Top queries
                cursor.execute('''
                    SELECT 
                        query,
                        COUNT(*) as frequency,
                        AVG(processing_time) as avg_time
                    FROM queries 
                    WHERE timestamp >= datetime('now', '-{} days')
                    GROUP BY query
                    ORDER BY frequency DESC
                    LIMIT 10
                '''.format(days))
                
                top_queries = [dict(row) for row in cursor.fetchall()]
                
                return {
                    "period_days": days,
                    "summary": stats,
                    "source_usage": source_stats,
                    "daily_volume": daily_volume,
                    "top_queries": top_queries,
                    "generated_at": datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error getting analytics: {e}")
            return {}
    
    def store_analytics_event(self, event_type: str, event_data: Dict[str, Any], 
                            user_id: str = None, session_id: str = None):
        """Store an analytics event"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO analytics (event_type, event_data, user_id, session_id)
                    VALUES (?, ?, ?, ?)
                ''', (
                    event_type,
                    json.dumps(event_data),
                    user_id,
                    session_id
                ))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error storing analytics event: {e}")
    
    def create_user_session(self, session_id: str, user_id: str = None, 
                          metadata: Dict[str, Any] = None) -> bool:
        """Create a new user session"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO user_sessions 
                    (session_id, user_id, start_time, last_activity, metadata)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    session_id,
                    user_id,
                    datetime.now().isoformat(),
                    datetime.now().isoformat(),
                    json.dumps(metadata or {})
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error creating user session: {e}")
            return False
    
    def update_session_activity(self, session_id: str, increment_query_count: bool = True):
        """Update session activity"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                if increment_query_count:
                    cursor.execute('''
                        UPDATE user_sessions 
                        SET last_activity = ?, query_count = query_count + 1
                        WHERE session_id = ?
                    ''', (datetime.now().isoformat(), session_id))
                else:
                    cursor.execute('''
                        UPDATE user_sessions 
                        SET last_activity = ?
                        WHERE session_id = ?
                    ''', (datetime.now().isoformat(), session_id))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error updating session activity: {e}")
    
    def cleanup_old_data(self, days_to_keep: int = 90):
        """Clean up old data to manage database size"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                cutoff_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                cutoff_date = cutoff_date.replace(day=cutoff_date.day - days_to_keep)
                
                # Clean old queries (cascades to related tables)
                cursor.execute('DELETE FROM queries WHERE timestamp < ?', (cutoff_date.isoformat(),))
                
                # Clean old analytics
                cursor.execute('DELETE FROM analytics WHERE timestamp < ?', (cutoff_date.isoformat(),))
                
                # Clean old sessions
                cursor.execute('DELETE FROM user_sessions WHERE last_activity < ?', (cutoff_date.isoformat(),))
                
                # Vacuum database to reclaim space
                cursor.execute('VACUUM')
                
                conn.commit()
                logger.info(f"Cleaned up data older than {days_to_keep} days")
                return True
                
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
            return False
    
    def export_data(self, output_path: str, format: str = "json") -> bool:
        """Export all data to file"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Get all data
                cursor.execute('SELECT * FROM queries')
                queries = [dict(row) for row in cursor.fetchall()]
                
                cursor.execute('SELECT * FROM query_results')
                results = [dict(row) for row in cursor.fetchall()]
                
                cursor.execute('SELECT * FROM query_responses')
                responses = [dict(row) for row in cursor.fetchall()]
                
                cursor.execute('SELECT * FROM analytics')
                analytics = [dict(row) for row in cursor.fetchall()]
                
                export_data = {
                    "export_timestamp": datetime.now().isoformat(),
                    "queries": queries,
                    "query_results": results,
                    "query_responses": responses,
                    "analytics": analytics
                }
                
                if format.lower() == "json":
                    with open(output_path, 'w') as f:
                        json.dump(export_data, f, indent=2)
                else:
                    # CSV export (simplified)
                    import pandas as pd
                    
                    # Export queries as CSV
                    queries_df = pd.DataFrame(queries)
                    queries_df.to_csv(output_path.replace('.csv', '_queries.csv'), index=False)
                    
                    # Export analytics as CSV
                    analytics_df = pd.DataFrame(analytics)
                    analytics_df.to_csv(output_path.replace('.csv', '_analytics.csv'), index=False)
                
                logger.info(f"Data exported to {output_path}")
                return True
                
        except Exception as e:
            logger.error(f"Error exporting data: {e}")
            return False
