"""
Foxa Trading Dashboard
Streamlit-based dashboard for the LLM Trading Assistant.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data import MarketData, TechnicalAnalyzer
from memory import TradingMemory, VectorMemory
from database import init_db, get_db_session, TradeRecommendation
from agents import TradingAnalyst, MultiAgentTradingCrew


# Page configuration
st.set_page_config(
    page_title="Foxa Trading Assistant",
    page_icon="🦊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    }
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(90deg, #ff6b35, #f7c831);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-card {
        background: rgba(255, 255, 255, 0.05);
        border-radius: 10px;
        padding: 1rem;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    .signal-buy { color: #00ff88; font-weight: bold; }
    .signal-sell { color: #ff4444; font-weight: bold; }
    .signal-hold { color: #ffaa00; font-weight: bold; }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_market_data():
    """Get market data instance (cached)."""
    return MarketData()


@st.cache_resource
def get_analyst():
    """Get trading analyst instance (cached)."""
    init_db()
    return TradingAnalyst()


@st.cache_resource
def get_multi_agent():
    """Get multi-agent crew (cached)."""
    return MultiAgentTradingCrew()


def create_candlestick_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    """Create a candlestick chart with volume."""
    fig = go.Figure()
    
    # Candlestick
    fig.add_trace(go.Candlestick(
        x=df.index,
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='Price',
        increasing_line_color='#00ff88',
        decreasing_line_color='#ff4444'
    ))
    
    # Volume
    colors = ['#00ff88' if c >= o else '#ff4444' 
              for c, o in zip(df['close'], df['open'])]
    
    fig.add_trace(go.Bar(
        x=df.index,
        y=df['volume'],
        name='Volume',
        marker_color=colors,
        opacity=0.3,
        yaxis='y2'
    ))
    
    # Layout
    fig.update_layout(
        title=f'{symbol} Price Chart',
        yaxis_title='Price (₹)',
        xaxis_title='Date',
        template='plotly_dark',
        height=500,
        yaxis2=dict(
            title='Volume',
            overlaying='y',
            side='right',
            showgrid=False
        ),
        xaxis_rangeslider_visible=False,
        legend=dict(orientation='h', y=1.1)
    )
    
    return fig


def render_quote_card(quote):
    """Render a quote card."""
    change_color = "green" if quote.change >= 0 else "red"
    change_icon = "📈" if quote.change >= 0 else "📉"
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Last Price",
            value=f"₹{quote.ltp:,.2f}",
            delta=f"{quote.change_pct:+.2f}%"
        )
    
    with col2:
        st.metric(
            label="Day High",
            value=f"₹{quote.high:,.2f}"
        )
    
    with col3:
        st.metric(
            label="Day Low",
            value=f"₹{quote.low:,.2f}"
        )
    
    with col4:
        st.metric(
            label="Volume",
            value=f"{quote.volume:,}"
        )


def render_signal_badge(signal: str):
    """Render a signal badge."""
    colors = {
        "BUY": ("🟢", "#00ff88"),
        "SELL": ("🔴", "#ff4444"),
        "HOLD": ("🟡", "#ffaa00"),
        "strong_buy": ("🟢🟢", "#00ff88"),
        "buy": ("🟢", "#00ff88"),
        "neutral": ("⚪", "#888888"),
        "sell": ("🔴", "#ff4444"),
        "strong_sell": ("🔴🔴", "#ff4444"),
    }
    
    icon, color = colors.get(signal.upper() if isinstance(signal, str) else signal, ("⚪", "#888888"))
    st.markdown(f"<span style='color: {color}; font-size: 1.5rem;'>{icon} {signal.upper()}</span>", 
                unsafe_allow_html=True)


def main():
    """Main dashboard function."""
    
    # Header
    st.markdown('<h1 class="main-header">🦊 Foxa Trading Assistant</h1>', unsafe_allow_html=True)
    st.markdown('<p style="text-align: center; color: #888;">LLM-Powered Analysis for Indian Markets</p>', 
                unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.image("https://via.placeholder.com/150x150/1a1a2e/ff6b35?text=🦊", width=100)
        st.title("Navigation")
        
        page = st.radio(
            "Select Page",
            ["📊 Dashboard", "🔍 Stock Analysis", "🤖 Multi-Agent", "💬 Chat", "📈 History"]
        )
        
        st.divider()
        st.markdown("### Quick Settings")
        
        use_multi_agent = st.toggle("Use Multi-Agent Analysis", value=False)
        
        st.divider()
        st.markdown("### Status")
        md = get_market_data()
        mode = "🟢 Live" if not md.use_mock else "🟡 Mock"
        st.markdown(f"Market Data: {mode}")
        
        analyst = get_analyst()
        st.markdown(f"LLM: {analyst.llm.model_name}")
    
    # Main content based on page selection
    if page == "📊 Dashboard":
        render_dashboard_page()
    elif page == "🔍 Stock Analysis":
        render_analysis_page()
    elif page == "🤖 Multi-Agent":
        render_multi_agent_page()
    elif page == "💬 Chat":
        render_chat_page()
    elif page == "📈 History":
        render_history_page()


def render_dashboard_page():
    """Render the main dashboard."""
    md = get_market_data()
    
    # Watchlist scanner
    st.subheader("📋 Market Scanner")
    
    symbols = ["RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "SBIN", 
               "BAJFINANCE", "BHARTIARTL", "KOTAKBANK", "ITC"]
    
    # Create grid
    cols = st.columns(5)
    
    for i, symbol in enumerate(symbols):
        quote = md.get_quote(symbol)
        if quote:
            with cols[i % 5]:
                with st.container():
                    change_color = "green" if quote.change >= 0 else "red"
                    st.markdown(f"**{symbol}**")
                    st.markdown(f"₹{quote.ltp:,.2f}")
                    st.markdown(f"<span style='color: {change_color}'>{quote.change_pct:+.2f}%</span>",
                               unsafe_allow_html=True)
    
    st.divider()
    
    # Quick analysis
    st.subheader("🎯 Quick Scan Results")
    
    analyst = get_analyst()
    scan_results = analyst.quick_scan(symbols[:5])
    
    # Display as dataframe
    if scan_results:
        df = pd.DataFrame(scan_results)
        
        # Style the dataframe
        def color_signal(val):
            if val in ['strong_buy', 'buy']:
                return 'color: #00ff88'
            elif val in ['strong_sell', 'sell']:
                return 'color: #ff4444'
            return 'color: #888888'
        
        styled_df = df.style.applymap(color_signal, subset=['signal'])
        st.dataframe(styled_df, use_container_width=True)


def render_analysis_page():
    """Render stock analysis page."""
    st.subheader("🔍 Stock Analysis")
    
    md = get_market_data()
    analyst = get_analyst()
    
    # Symbol input
    col1, col2 = st.columns([2, 1])
    with col1:
        symbol = st.text_input("Enter Stock Symbol", value="RELIANCE").upper()
    with col2:
        analyze_btn = st.button("🔍 Analyze", type="primary", use_container_width=True)
    
    if analyze_btn or symbol:
        with st.spinner(f"Analyzing {symbol}..."):
            # Get quote
            quote = md.get_quote(symbol)
            if not quote:
                st.error(f"Could not fetch data for {symbol}")
                return
            
            # Quote card
            st.markdown("### Current Price")
            render_quote_card(quote)
            
            # Chart
            st.markdown("### Price Chart")
            hist = md.get_historical(symbol, days=60)
            if hist is not None and not hist.empty:
                fig = create_candlestick_chart(hist, symbol)
                st.plotly_chart(fig, use_container_width=True)
            
            # Technical Analysis
            st.markdown("### Technical Analysis")
            
            if hist is not None and not hist.empty:
                ta = TechnicalAnalyzer(hist)
                summary = ta.get_full_analysis(symbol)
                
                col1, col2 = st.columns([1, 2])
                
                with col1:
                    st.markdown("#### Overall Signal")
                    render_signal_badge(summary.overall_signal.value)
                    st.metric("Bullish Signals", summary.bullish_count)
                    st.metric("Bearish Signals", summary.bearish_count)
                
                with col2:
                    st.markdown("#### Indicator Details")
                    for name, ind in summary.indicators.items():
                        st.markdown(f"**{name}**: {ind.value:.2f} - {ind.description}")
            
            # LLM Analysis
            if analyze_btn:
                st.divider()
                st.markdown("### 🤖 AI Analysis")
                with st.spinner("Getting AI analysis..."):
                    result = analyst.analyze_stock(symbol)
                    
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.markdown("#### Recommendation")
                        render_signal_badge(result.signal)
                        st.metric("Confidence", f"{result.confidence:.0f}%")
                        if result.entry_price:
                            st.metric("Entry", f"₹{result.entry_price:,.2f}")
                        if result.target_price:
                            st.metric("Target", f"₹{result.target_price:,.2f}")
                        if result.stop_loss:
                            st.metric("Stop Loss", f"₹{result.stop_loss:,.2f}")
                    
                    with col2:
                        st.markdown("#### Reasoning")
                        st.markdown(result.reasoning)


def render_multi_agent_page():
    """Render multi-agent analysis page."""
    st.subheader("🤖 Multi-Agent Analysis")
    st.markdown("*Get comprehensive analysis from specialized AI agents*")
    
    md = get_market_data()
    crew = get_multi_agent()
    
    symbol = st.text_input("Enter Stock Symbol", value="TCS", key="multi_symbol").upper()
    
    if st.button("🚀 Run Multi-Agent Analysis", type="primary"):
        with st.spinner(f"Running multi-agent analysis for {symbol}... This may take a minute."):
            # Get data
            quote = md.get_quote(symbol)
            hist = md.get_historical(symbol, days=60)
            
            if not quote or hist is None:
                st.error("Could not fetch data")
                return
            
            ta = TechnicalAnalyzer(hist)
            summary = ta.get_full_analysis(symbol)
            
            quote_data = f"""
Last Price: ₹{quote.ltp:,.2f}
Change: {quote.change:+.2f} ({quote.change_pct:+.2f}%)
High: ₹{quote.high:,.2f}
Low: ₹{quote.low:,.2f}
Volume: {quote.volume:,}
"""
            
            results = crew.analyze_stock(
                symbol=symbol,
                technical_data=summary.analysis_text,
                quote_data=quote_data
            )
            
            # Display results
            st.markdown("### Final Decision")
            fd = results["final_decision"]
            
            col1, col2, col3 = st.columns(3)
            with col1:
                render_signal_badge(fd["signal"])
            with col2:
                st.metric("Confidence", f"{fd['confidence']}%")
            with col3:
                if fd["position_size"]:
                    st.metric("Position Size", f"{fd['position_size']}%")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                if fd["entry"]:
                    st.metric("Entry", f"₹{fd['entry']:,.2f}")
            with col2:
                if fd["target"]:
                    st.metric("Target", f"₹{fd['target']:,.2f}")
            with col3:
                if fd["stop_loss"]:
                    st.metric("Stop Loss", f"₹{fd['stop_loss']:,.2f}")
            
            st.markdown("**Rationale:**")
            st.markdown(fd["rationale"])
            
            # Agent perspectives
            st.divider()
            st.markdown("### Agent Perspectives")
            
            with st.expander("📊 Technical Analyst"):
                st.markdown(results["agents"]["technical"])
            
            with st.expander("📰 Sentiment Analyst"):
                st.markdown(results["agents"]["sentiment"])
            
            with st.expander("📈 Bull Researcher"):
                st.markdown(results["agents"]["bull_case"])
            
            with st.expander("📉 Bear Researcher"):
                st.markdown(results["agents"]["bear_case"])
            
            with st.expander("⚠️ Risk Manager"):
                st.markdown(results["agents"]["risk"])


def render_chat_page():
    """Render chat interface."""
    st.subheader("💬 Chat with Trading Assistant")
    
    analyst = get_analyst()
    
    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask about stocks, trading strategies, or market analysis..."):
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Get response
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                response = analyst.chat(prompt)
                st.markdown(response)
        
        # Add assistant message
        st.session_state.messages.append({"role": "assistant", "content": response})


def render_history_page():
    """Render history page."""
    st.subheader("📈 Recommendation History")
    
    try:
        with get_db_session() as db:
            recommendations = db.query(TradeRecommendation).order_by(
                TradeRecommendation.created_at.desc()
            ).limit(20).all()
            
            if not recommendations:
                st.info("No recommendations yet. Analyze some stocks to see history.")
                return
            
            for rec in recommendations:
                with st.expander(f"{rec.symbol} - {rec.direction} ({rec.created_at.strftime('%Y-%m-%d %H:%M')})"):
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Signal", rec.direction)
                    with col2:
                        st.metric("Confidence", f"{rec.confidence*100:.0f}%" if rec.confidence else "N/A")
                    with col3:
                        st.metric("Entry", f"₹{rec.entry_price:,.2f}" if rec.entry_price else "N/A")
                    with col4:
                        st.metric("Status", rec.status)
                    
                    if rec.reasoning:
                        st.markdown("**Reasoning:**")
                        st.markdown(rec.reasoning[:500] + "..." if len(rec.reasoning) > 500 else rec.reasoning)
    except Exception as e:
        st.error(f"Could not load history: {e}")


if __name__ == "__main__":
    main()
