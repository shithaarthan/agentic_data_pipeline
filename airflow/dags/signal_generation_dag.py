"""
Airflow DAG for Signal Generation
Runs Kimi scanner and generates trading signals.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from lakehouse.gold import GoldAnalytics


def generate_signals(**context):
    """Run signal generation."""
    gold = GoldAnalytics()
    count = gold.generate_signals()
    return {'signals_generated': count}


def generate_market_summary(**context):
    """Generate daily market summary."""
    gold = GoldAnalytics()
    summary = gold.generate_market_summary()
    return summary


def run_agent_analysis(**context):
    """Run LangGraph agent analysis on top signals."""
    from agents.langgraph_workflow import TradingAgentWorkflow
    
    # Get top signals
    gold = GoldAnalytics()
    signals_df = gold.get_latest_signals(min_confidence='HIGH', limit=10)
    
    if signals_df.empty:
        return {'message': 'No high confidence signals to analyze'}
    
    workflow = TradingAgentWorkflow()
    results = []
    
    for symbol in signals_df['symbol'].unique():
        result = workflow.analyze(symbol)
        results.append(result)
    
    return {'analysis_results': results}


with DAG(
    'signal_generation',
    default_args={
        'owner': 'foxa',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Generate trading signals and run agent analysis',
    schedule_interval='30 18 * * *',  # 6:30 PM (after pipeline)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['signals', 'agents', 'trading'],
) as dag:
    
    # Task 1: Generate signals
    signals_task = PythonOperator(
        task_id='generate_signals',
        python_callable=generate_signals,
    )
    
    # Task 2: Market summary
    summary_task = PythonOperator(
        task_id='market_summary',
        python_callable=generate_market_summary,
    )
    
    # Task 3: Agent analysis
    agent_task = PythonOperator(
        task_id='agent_analysis',
        python_callable=run_agent_analysis,
    )
    
    # Parallel execution for signals and summary, then agents
    [signals_task, summary_task] >> agent_task