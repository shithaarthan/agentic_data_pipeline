"""
Data Loader Node
Loads market data and prepares it for agent analysis.
"""

from datetime import datetime
from typing import Dict, Any
from loguru import logger

from agents.state import AgentState
from data.market_data import MarketData
from data.technical_indicators import TechnicalAnalyzer
from data.knowledge import KnowledgeReader


def data_loader_node(state: AgentState) -> Dict[str, Any]:
    """
    Load all necessary data for analysis.
    
    This is the entry point of the workflow.
    """
    symbol = state['symbol']
    logger.info(f"[DataLoader] Loading data for {symbol}")
    
    try:
        md = MarketData()
        kb = KnowledgeReader()
        
        # Load quote
        quote = md.get_quote(symbol)
        quote_data = None
        if quote:
            quote_data = {
                'symbol': quote.symbol,
                'ltp': quote.ltp,
                'change': quote.change,
                'change_pct': quote.change_pct,
                'volume': quote.volume,
                'high': quote.high,
                'low': quote.low,
                'open': quote.open
            }
        
        # Load OHLCV
        hist = md.get_historical(symbol, days=100)
        ohlcv_data = None
        technical_indicators = None
        
        if hist is not None and not hist.empty:
            ohlcv_data = {
                'recent_dates': hist.index[-10:].strftime('%Y-%m-%d').tolist(),
                'recent_closes': hist['close'].tail(10).tolist(),
                'recent_volumes': hist['volume'].tail(10).tolist(),
                'high_52w': hist['high'].max(),
                'low_52w': hist['low'].min(),
                'avg_volume': hist['volume'].mean()
            }
            
            # Calculate technical indicators
            analyzer = TechnicalAnalyzer(hist)
            latest = analyzer.get_latest()
            summary = analyzer.get_full_analysis(symbol)
            
            technical_indicators = {
                'rsi': latest.get('rsi'),
                'macd': latest.get('macd'),
                'macd_signal': latest.get('macd_signal'),
                'sma_20': latest.get('sma_20'),
                'sma_50': latest.get('sma_50'),
                'sma_200': latest.get('sma_200'),
                'bb_upper': latest.get('bb_upper'),
                'bb_lower': latest.get('bb_lower'),
                'analysis_summary': summary.analysis_text if summary else None
            }
        
        # Load fundamentals
        fund_content = kb.get_stock(symbol)
        fundamentals = None
        if fund_content:
            # Extract key sections
            fundamentals = {
                'raw_content': fund_content[:2000],  # Truncate for LLM
                'has_fundamental_analysis': 'Agent: Fundamental Analysis' in fund_content
            }
        
        # Load sector info
        sector_content = kb.get_sector('IT')  # Default, should detect from stock
        sector_data = None
        if sector_content:
            sector_data = {'content': sector_content[:1000]}
        
        logger.success(f"[DataLoader] Data loaded for {symbol}")
        
        return {
            'quote_data': quote_data,
            'ohlcv_data': ohlcv_data,
            'technical_indicators': technical_indicators,
            'fundamentals': fundamentals,
            'sector_data': sector_data
        }
        
    except Exception as e:
        logger.error(f"[DataLoader] Error loading data: {e}")
        return {'errors': [f"Data loading failed: {str(e)}"]}


def technical_analyst_node(state: AgentState) -> Dict[str, Any]:
    """
    Technical Analyst Agent
    Analyzes technical indicators and chart patterns.
    """
    symbol = state['symbol']
    indicators = state.get('technical_indicators', {})
    
    logger.info(f"[TechnicalAnalyst] Analyzing {symbol}")
    
    # Simple rule-based analysis (replace with LLM in production)
    rsi = indicators.get('rsi', 50)
    macd = indicators.get('macd', 0)
    macd_signal = indicators.get('macd_signal', 0)
    
    # Determine signal
    if rsi < 30 and macd > macd_signal:
        signal = 'BUY'
        confidence = 75
        reasoning = f"RSI oversold ({rsi:.1f}) with MACD bullish crossover"
    elif rsi > 70 and macd < macd_signal:
        signal = 'SELL'
        confidence = 70
        reasoning = f"RSI overbought ({rsi:.1f}) with MACD bearish crossover"
    else:
        signal = 'HOLD'
        confidence = 50
        reasoning = f"Mixed signals: RSI={rsi:.1f}, MACD trend unclear"
    
    message = {
        'role': 'technical',
        'content': f"Technical Analysis: {signal} with {confidence}% confidence",
        'timestamp': datetime.now().isoformat(),
        'recommendation': signal,
        'confidence': confidence,
        'reasoning': reasoning
    }
    
    logger.info(f"[TechnicalAnalyst] {signal} ({confidence}%)")
    
    return {
        'messages': [message],
        'technical_signal': signal,
        'technical_confidence': confidence
    }


def fundamental_analyst_node(state: AgentState) -> Dict[str, Any]:
    """
    Fundamental Analyst Agent
    Analyzes company fundamentals and financial health.
    """
    symbol = state['symbol']
    fundamentals = state.get('fundamentals', {})
    
    logger.info(f"[FundamentalAnalyst] Analyzing {symbol}")
    
    # Check if we have fundamental analysis
    has_analysis = fundamentals.get('has_fundamental_analysis', False)
    
    if has_analysis:
        # Parse score from content if available
        content = fundamentals.get('raw_content', '')
        
        # Simple parsing for demonstration
        if 'Fundamental Score: ' in content:
            try:
                score_line = [l for l in content.split('\n') if 'Fundamental Score:' in l][0]
                score = float(score_line.split(':')[1].split('/')[0].strip())
            except:
                score = 5.0
        else:
            score = 5.0
        
        if score >= 7:
            signal = 'BUY'
            confidence = min(60 + (score - 7) * 10, 90)
        elif score <= 4:
            signal = 'SELL'
            confidence = min(60 + (4 - score) * 10, 80)
        else:
            signal = 'HOLD'
            confidence = 50
        
        reasoning = f"Fundamental score {score}/10"
    else:
        signal = 'HOLD'
        confidence = 40
        reasoning = "No fundamental data available"
        score = None
    
    message = {
        'role': 'fundamental',
        'content': f"Fundamental Analysis: {signal} with {confidence}% confidence",
        'timestamp': datetime.now().isoformat(),
        'recommendation': signal,
        'confidence': confidence,
        'reasoning': reasoning
    }
    
    logger.info(f"[FundamentalAnalyst] {signal} ({confidence}%)")
    
    return {
        'messages': [message],
        'fundamental_signal': signal,
        'fundamental_confidence': confidence,
        'fundamental_score': score
    }


def risk_manager_node(state: AgentState) -> Dict[str, Any]:
    """
    Risk Manager Agent
    Assesses position sizing and risk parameters.
    """
    symbol = state['symbol']
    technical = state.get('technical_signal', 'HOLD')
    fundamental = state.get('fundamental_signal', 'HOLD')
    
    logger.info(f"[RiskManager] Assessing risk for {symbol}")
    
    # Check signal alignment
    if technical == fundamental:
        risk_level = 'LOW'
        assessment = "Signals aligned - standard position size"
        position_pct = 5.0  # 5% of portfolio
    elif technical == 'HOLD' or fundamental == 'HOLD':
        risk_level = 'MEDIUM'
        assessment = "Mixed signals - reduced position size"
        position_pct = 2.5
    else:
        risk_level = 'HIGH'
        assessment = "Conflicting signals - avoid or minimal position"
        position_pct = 1.0
    
    message = {
        'role': 'risk',
        'content': f"Risk Assessment: {risk_level} - {assessment}",
        'timestamp': datetime.now().isoformat(),
        'recommendation': None,
        'confidence': None,
        'reasoning': assessment
    }
    
    logger.info(f"[RiskManager] Risk level: {risk_level}")
    
    return {
        'messages': [message],
        'risk_assessment': assessment,
        'risk_level': risk_level,
        'trade_parameters': {'position_size': position_pct}
    }


def macro_analyst_node(state: AgentState) -> Dict[str, Any]:
    """
    Macro Analyst Agent
    Considers broader market context.
    """
    symbol = state['symbol']
    sector = state.get('sector_data', {})
    
    logger.info(f"[MacroAnalyst] Analyzing macro context for {symbol}")
    
    # Simplified macro analysis
    # In production, this would query market breadth indicators
    sentiment = 'NEUTRAL'
    context = "Market conditions appear neutral"
    
    message = {
        'role': 'macro',
        'content': f"Macro Context: {sentiment}",
        'timestamp': datetime.now().isoformat(),
        'recommendation': None,
        'confidence': None,
        'reasoning': context
    }
    
    logger.info(f"[MacroAnalyst] Sentiment: {sentiment}")
    
    return {
        'messages': [message],
        'macro_context': context,
        'macro_sentiment': sentiment
    }


def trader_node(state: AgentState) -> Dict[str, Any]:
    """
    Trader Agent
    Makes final decision and sets trade parameters.
    """
    symbol = state['symbol']
    quote = state.get('quote_data', {})
    
    logger.info(f"[Trader] Making final decision for {symbol}")
    
    # Gather all signals
    tech = state.get('technical_signal', 'HOLD')
    fund = state.get('fundamental_signal', 'HOLD')
    risk = state.get('risk_level', 'MEDIUM')
    
    tech_conf = state.get('technical_confidence', 50)
    fund_conf = state.get('fundamental_confidence', 50)
    
    # Weighted decision
    # Technical: 40%, Fundamental: 40%, Risk: 20%
    signal_weights = {'STRONG_BUY': 2, 'BUY': 1, 'HOLD': 0, 'SELL': -1, 'STRONG_SELL': -2}
    
    tech_score = signal_weights.get(tech, 0) * (tech_conf / 100) * 0.4
    fund_score = signal_weights.get(fund, 0) * (fund_conf / 100) * 0.4
    
    # Risk adjustment
    risk_multiplier = {'LOW': 1.0, 'MEDIUM': 0.7, 'HIGH': 0.4}.get(risk, 0.7)
    
    final_score = (tech_score + fund_score) * risk_multiplier
    
    # Determine final recommendation
    if final_score >= 1.0:
        final_rec = 'STRONG_BUY'
        final_conf = min(90, (tech_conf + fund_conf) / 2 * risk_multiplier)
    elif final_score >= 0.3:
        final_rec = 'BUY'
        final_conf = min(75, (tech_conf + fund_conf) / 2 * risk_multiplier)
    elif final_score <= -1.0:
        final_rec = 'STRONG_SELL'
        final_conf = min(85, (tech_conf + fund_conf) / 2 * risk_multiplier)
    elif final_score <= -0.3:
        final_rec = 'SELL'
        final_conf = min(70, (tech_conf + fund_conf) / 2 * risk_multiplier)
    else:
        final_rec = 'HOLD'
        final_conf = 50
    
    # Calculate trade parameters
    ltp = quote.get('ltp', 0)
    if final_rec in ['BUY', 'STRONG_BUY'] and ltp > 0:
        entry = ltp
        stop = ltp * 0.95  # 5% stop
        target = ltp * 1.10  # 10% target
        rr = 2.0
    elif final_rec in ['SELL', 'STRONG_SELL'] and ltp > 0:
        entry = ltp
        stop = ltp * 1.05
        target = ltp * 0.90
        rr = 2.0
    else:
        entry = target = stop = rr = None
    
    trade_params = {
        'entry_price': entry,
        'target_price': target,
        'stop_loss': stop,
        'risk_reward_ratio': rr
    }
    
    message = {
        'role': 'trader',
        'content': f"FINAL DECISION: {final_rec} ({final_conf:.0f}% confidence)",
        'timestamp': datetime.now().isoformat(),
        'recommendation': final_rec,
        'confidence': round(final_conf, 1),
        'reasoning': f"Technical: {tech}, Fundamental: {fund}, Risk: {risk}"
    }
    
    logger.success(f"[Trader] Decision: {final_rec} ({final_conf:.0f}%)")
    
    return {
        'messages': [message],
        'final_recommendation': final_rec,
        'final_confidence': round(final_conf, 1),
        'trade_parameters': trade_params,
        'workflow_end': datetime.now().isoformat()
    }