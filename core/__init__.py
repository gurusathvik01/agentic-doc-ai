"""
Agentic Doc AI Core Module

This module provides the core functionality for the Agentic Document AI system,
integrating PageIndex, Ollama LLM, and multi-dataset query capabilities.
"""

from .orchestrator import AgenticOrchestrator
from .llm_engine import OllamaLLMEngine
from .page_index_engine import PageIndexEngine
from .execution_planner import ExecutionPlanner
from .query_engine import QueryEngine
from .merge_layer import MergeLayer

__all__ = [
    'AgenticOrchestrator',
    'OllamaLLMEngine',
    'PageIndexEngine',
    'ExecutionPlanner',
    'QueryEngine',
    'MergeLayer'
]

__version__ = "1.0.0"
