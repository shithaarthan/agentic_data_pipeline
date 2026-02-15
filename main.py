"""
Foxa Trading Assistant - Enhanced Main Entry Point
An LLM-powered trading analysis assistant for Indian markets.
"""

import sys
import os
from datetime import datetime
from loguru import logger
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.markdown import Markdown
from rich.prompt import Prompt, Confirm

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import settings
from database import init_db
from data import MarketData, TechnicalAnalyzer
from memory import TradingMemory
from agents import TradingAnalyst, MultiAgentTradingCrew


# Configure logging
logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    level=settings.log_level
)

console = Console()


def print_header():
    """Print application header."""
    header = """
[bold cyan]╔═══════════════════════════════════════════════════════════════╗
║              🦊 FOXA TRADING ASSISTANT                        ║
║              LLM-Powered Indian Market Analysis               ║
╚═══════════════════════════════════════════════════════════════╝[/bold cyan]
    """
    console.print(header)


def show_quote(symbol: str, md: MarketData):
    """Display quote for a symbol."""
    quote = md.get_quote(symbol)
    if not quote:
        console.print(f"[red]Could not fetch quote for {symbol}[/red]")
        return
    
    change_color = "green" if quote.change >= 0 else "red"
    change_symbol = "▲" if quote.change >= 0 else "▼"
    
    table = Table(title=f"📊 {quote.symbol} ({quote.exchange})", show_header=False)
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="white")
    
    table.add_row("Last Price", f"₹{quote.ltp:,.2f}")
    table.add_row("Change", f"[{change_color}]{change_symbol} ₹{quote.change:,.2f} ({quote.change_pct:+.2f}%)[/{change_color}]")
    table.add_row("Open", f"₹{quote.open:,.2f}")
    table.add_row("High", f"₹{quote.high:,.2f}")
    table.add_row("Low", f"₹{quote.low:,.2f}")
    table.add_row("Volume", f"{quote.volume:,}")
    
    console.print(table)


def show_technical_analysis(symbol: str, md: MarketData):
    """Display technical analysis for a symbol."""
    console.print(f"\n[cyan]Fetching data for {symbol}...[/cyan]")
    
    df = md.get_historical(symbol, days=100)
    if df is None or df.empty:
        console.print(f"[red]Could not fetch historical data for {symbol}[/red]")
        return
    
    analyzer = TechnicalAnalyzer(df)
    summary = analyzer.get_full_analysis(symbol)
    
    console.print(Panel(
        summary.analysis_text,
        title=f"Technical Analysis - {symbol}",
        border_style="cyan"
    ))


def ai_analyze(symbol: str, analyst: TradingAnalyst):
    """Run AI analysis on a stock."""
    console.print(f"\n[yellow]🤖 Running AI analysis for {symbol}...[/yellow]")
    
    result = analyst.analyze_stock(symbol)
    
    signal_color = {"BUY": "green", "SELL": "red", "HOLD": "yellow"}.get(result.signal, "white")
    
    console.print(f"\n[bold {signal_color}]Signal: {result.signal}[/bold {signal_color}] (Confidence: {result.confidence:.0f}%)")
    
    if result.entry_price:
        console.print(f"Entry: ₹{result.entry_price:,.2f}")
    if result.target_price:
        console.print(f"Target: ₹{result.target_price:,.2f}")
    if result.stop_loss:
        console.print(f"Stop Loss: ₹{result.stop_loss:,.2f}")
    
    console.print(Panel(result.reasoning, title="Analysis", border_style="cyan"))


def multi_agent_analyze(symbol: str, md: MarketData, crew: MultiAgentTradingCrew):
    """Run multi-agent analysis."""
    console.print(f"\n[yellow]🤖🤖🤖 Running Multi-Agent analysis for {symbol}...[/yellow]")
    console.print("[dim]This may take a minute as multiple agents discuss...[/dim]")
    
    quote = md.get_quote(symbol)
    hist = md.get_historical(symbol, days=60)
    
    if not quote or hist is None:
        console.print("[red]Could not fetch data[/red]")
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
    
    fd = results["final_decision"]
    signal_color = {"BUY": "green", "SELL": "red", "HOLD": "yellow"}.get(fd["signal"], "white")
    
    console.print(f"\n[bold {signal_color}]═══ FINAL DECISION: {fd['signal']} ═══[/bold {signal_color}]")
    console.print(f"Confidence: {fd['confidence']}%")
    
    if fd["entry"]:
        console.print(f"Entry: ₹{fd['entry']:,.2f}")
    if fd["target"]:
        console.print(f"Target: ₹{fd['target']:,.2f}")
    if fd["stop_loss"]:
        console.print(f"Stop Loss: ₹{fd['stop_loss']:,.2f}")
    
    console.print(Panel(fd["rationale"], title="Rationale", border_style="cyan"))
    
    if Confirm.ask("\nShow full agent discussion?"):
        console.print(crew.get_discussion_transcript())


def quick_scan(analyst: TradingAnalyst):
    """Run quick market scan."""
    console.print("\n[cyan]Running quick market scan...[/cyan]")
    
    results = analyst.quick_scan()
    
    table = Table(title="📊 Market Scan")
    table.add_column("Symbol", style="cyan")
    table.add_column("LTP", justify="right")
    table.add_column("Change", justify="right")
    table.add_column("Signal", justify="center")
    table.add_column("Bull", justify="right", style="green")
    table.add_column("Bear", justify="right", style="red")
    
    for r in results:
        change_color = "green" if r["change_pct"] >= 0 else "red"
        signal_color = {"strong_buy": "bold green", "buy": "green", 
                        "strong_sell": "bold red", "sell": "red",
                        "neutral": "yellow"}.get(r["signal"], "white")
        
        table.add_row(
            r["symbol"],
            f"₹{r['ltp']:,.2f}",
            f"[{change_color}]{r['change_pct']:+.2f}%[/{change_color}]",
            f"[{signal_color}]{r['signal'].upper()}[/{signal_color}]",
            str(r["bullish"]),
            str(r["bearish"])
        )
    
    console.print(table)


def interactive_mode():
    """Run interactive CLI mode."""
    md = MarketData()
    memory = TradingMemory()
    analyst = TradingAnalyst()
    crew = None  # Lazy load
    
    help_text = """
[bold]Available Commands:[/bold]

| Command | Description |
|---------|-------------|
| [cyan]quote <SYMBOL>[/cyan] | Get current price quote |
| [cyan]tech <SYMBOL>[/cyan] | Technical analysis |
| [cyan]ai <SYMBOL>[/cyan] | AI-powered analysis |
| [cyan]crew <SYMBOL>[/cyan] | Multi-agent analysis |
| [cyan]scan[/cyan] | Quick market scan |
| [cyan]chat[/cyan] | Free-form chat with AI |
| [cyan]dashboard[/cyan] | Launch web dashboard |
| [cyan]symbols[/cyan] | List available symbols |
| [cyan]clear[/cyan] | Clear screen |
| [cyan]quit[/cyan] | Exit |
"""
    
    console.print(Markdown(help_text))
    console.print()
    
    while True:
        try:
            user_input = console.input("[bold cyan]foxa>[/bold cyan] ").strip()
            
            if not user_input:
                continue
            
            parts = user_input.split()
            command = parts[0].lower()
            
            if command in ["quit", "exit", "q"]:
                console.print("[yellow]Goodbye! Happy trading! 🦊[/yellow]")
                break
            
            elif command == "help":
                console.print(Markdown(help_text))
            
            elif command == "quote" and len(parts) > 1:
                show_quote(parts[1].upper(), md)
            
            elif command == "tech" and len(parts) > 1:
                show_technical_analysis(parts[1].upper(), md)
            
            elif command == "ai" and len(parts) > 1:
                ai_analyze(parts[1].upper(), analyst)
            
            elif command == "crew" and len(parts) > 1:
                if crew is None:
                    console.print("[dim]Initializing multi-agent crew...[/dim]")
                    crew = MultiAgentTradingCrew()
                multi_agent_analyze(parts[1].upper(), md, crew)
            
            elif command == "scan":
                quick_scan(analyst)
            
            elif command == "chat":
                console.print("[cyan]Chat mode. Type 'exit' to return.[/cyan]")
                while True:
                    msg = console.input("[dim]you>[/dim] ").strip()
                    if msg.lower() == "exit":
                        break
                    response = analyst.chat(msg)
                    console.print(Panel(response, title="🦊 Foxa", border_style="cyan"))
            
            elif command == "dashboard":
                console.print("[cyan]Launching dashboard...[/cyan]")
                console.print("[dim]Run: streamlit run dashboard/app.py[/dim]")
                import subprocess
                subprocess.Popen([sys.executable, "-m", "streamlit", "run", "dashboard/app.py"])
            
            elif command == "symbols":
                symbols = md.available_symbols
                console.print(f"[cyan]Available symbols:[/cyan] {', '.join(symbols)}")
            
            elif command == "clear":
                console.clear()
                print_header()
            
            else:
                console.print(f"[yellow]Unknown command: {command}. Type 'help' for commands.[/yellow]")
        
        except KeyboardInterrupt:
            console.print("\n[yellow]Use 'quit' to exit.[/yellow]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
            logger.error(f"Error: {e}")


def main():
    """Main entry point."""
    print_header()
    
    # Initialize
    console.print("[cyan]Initializing...[/cyan]")
    init_db()
    console.print("[green]✓ Database ready[/green]")
    
    md = MarketData()
    mode = "MOCK" if md.use_mock else "LIVE (Angel One)"
    console.print(f"[green]✓ Market data ({mode})[/green]")
    
    console.print(f"[dim]Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/dim]\n")
    
    # Run interactive mode
    interactive_mode()


if __name__ == "__main__":
    main()
