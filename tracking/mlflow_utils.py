"""
MLflow Integration for Foxa Trading Platform
Tracks experiments, agent performance, and model versions.
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
from contextlib import contextmanager

sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger


class MLflowTracker:
    """
    MLflow tracking wrapper for Foxa.
    Tracks agent runs, signal performance, and model versions.
    """
    
    def __init__(self, tracking_uri: str = "file:./mlruns", experiment_name: str = "foxa_trading"):
        self.tracking_uri = tracking_uri
        self.experiment_name = experiment_name
        self._mlflow = None
        self._experiment_id = None
    
    def _get_mlflow(self):
        """Lazy import MLflow."""
        if self._mlflow is None:
            try:
                import mlflow
                mlflow.set_tracking_uri(self.tracking_uri)
                self._mlflow = mlflow
                
                # Create or get experiment
                exp = mlflow.get_experiment_by_name(self.experiment_name)
                if exp is None:
                    self._experiment_id = mlflow.create_experiment(self.experiment_name)
                else:
                    self._experiment_id = exp.experiment_id
                    
                logger.info(f"MLflow initialized: {self.tracking_uri}")
            except ImportError:
                logger.error("MLflow not installed. Run: pip install mlflow")
                raise
        return self._mlflow
    
    @contextmanager
    def start_run(self, run_name: Optional[str] = None, tags: Optional[Dict] = None):
        """Context manager for MLflow runs."""
        mlflow = self._get_mlflow()
        
        with mlflow.start_run(
            experiment_id=self._experiment_id,
            run_name=run_name
        ) as run:
            if tags:
                mlflow.set_tags(tags)
            yield run
    
    def log_agent_analysis(
        self,
        symbol: str,
        result: Dict[str, Any],
        run_name: Optional[str] = None
    ):
        """
        Log LangGraph agent analysis to MLflow.
        
        Args:
            symbol: Stock symbol analyzed
            result: Analysis result from TradingAgentWorkflow
            run_name: Optional custom run name
        """
        try:
            mlflow = self._get_mlflow()
            
            run_name = run_name or f"analysis_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            with self.start_run(run_name=run_name, tags={
                'symbol': symbol,
                'agent_type': 'langgraph',
                'workflow': 'multi_agent'
            }) as run:
                
                # Log parameters
                mlflow.log_param('symbol', symbol)
                mlflow.log_param('final_recommendation', result.get('final_recommendation'))
                mlflow.log_param('technical_signal', result.get('technical_signal'))
                mlflow.log_param('fundamental_signal', result.get('fundamental_signal'))
                mlflow.log_param('risk_level', result.get('risk_level'))
                
                # Log metrics
                mlflow.log_metric('final_confidence', result.get('final_confidence', 0))
                mlflow.log_metric('fundamental_score', result.get('fundamental_score') or 0)
                
                # Log trade parameters if available
                params = result.get('trade_parameters', {})
                if params.get('entry_price'):
                    mlflow.log_metric('entry_price', params['entry_price'])
                    mlflow.log_metric('target_price', params['target_price'])
                    mlflow.log_metric('stop_loss', params['stop_loss'])
                    mlflow.log_metric('risk_reward_ratio', params.get('risk_reward_ratio', 0))
                
                # Log artifacts (full result as JSON)
                import json
                artifact_path = f"/tmp/mlflow_artifact_{symbol}.json"
                with open(artifact_path, 'w') as f:
                    json.dump(result, f, indent=2, default=str)
                mlflow.log_artifact(artifact_path)
                
                logger.info(f"Logged agent analysis for {symbol} to MLflow")
                
        except Exception as e:
            logger.error(f"Failed to log to MLflow: {e}")
    
    def log_signal_performance(
        self,
        signal: Dict[str, Any],
        outcome: Optional[str] = None,
        actual_return: Optional[float] = None
    ):
        """
        Log signal performance metrics.
        
        Args:
            signal: Signal dict with symbol, entry, target, stop
            outcome: 'hit_target', 'hit_stop', 'open'
            actual_return: Actual return % realized
        """
        try:
            mlflow = self._get_mlflow()
            
            symbol = signal.get('symbol', 'UNKNOWN')
            run_name = f"signal_{symbol}_{datetime.now().strftime('%Y%m%d')}"
            
            with self.start_run(run_name=run_name, tags={
                'symbol': symbol,
                'signal_type': signal.get('strategy', 'unknown'),
                'outcome': outcome or 'pending'
            }) as run:
                
                # Log signal parameters
                mlflow.log_param('symbol', symbol)
                mlflow.log_param('strategy', signal.get('strategy'))
                mlflow.log_param('signal_type', signal.get('signal'))
                mlflow.log_param('outcome', outcome)
                
                # Log metrics
                mlflow.log_metric('entry_price', signal.get('entry', 0))
                mlflow.log_metric('target_price', signal.get('target', 0))
                mlflow.log_metric('stop_loss', signal.get('stop', 0))
                mlflow.log_metric('risk_reward', signal.get('risk_reward', 0))
                
                if actual_return is not None:
                    mlflow.log_metric('actual_return', actual_return)
                
                logger.info(f"Logged signal performance for {symbol}")
                
        except Exception as e:
            logger.error(f"Failed to log signal: {e}")
    
    def get_experiment_runs(self, num_runs: int = 10):
        """Get recent experiment runs."""
        try:
            mlflow = self._get_mlflow()
            runs = mlflow.search_runs(
                experiment_ids=[self._experiment_id],
                order_by=["start_time DESC"],
                max_results=num_runs
            )
            return runs
        except Exception as e:
            logger.error(f"Failed to get runs: {e}")
            return None


# ============================================================
# Singleton Instance
# ============================================================

_tracker: Optional[MLflowTracker] = None


def get_tracker() -> MLflowTracker:
    """Get or create singleton tracker instance."""
    global _tracker
    if _tracker is None:
        _tracker = MLflowTracker()
    return _tracker


# ============================================================
# CLI Interface
# ============================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="MLflow Tracking Utils")
    parser.add_argument("--list-runs", action="store_true", help="List recent runs")
    parser.add_argument("--num-runs", type=int, default=10, help="Number of runs to show")
    
    args = parser.parse_args()
    
    tracker = get_tracker()
    
    if args.list_runs:
        import json
        runs = tracker.get_experiment_runs(args.num_runs)
        if runs is not None:
            print(runs.to_json(orient='records', indent=2))
        else:
            print("No runs found")