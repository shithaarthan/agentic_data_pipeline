"""
Nifty 500 Stock Data Loader
Fetches and populates the stocks table with Nifty 500 constituents.
"""

import csv
import os
import sys
from datetime import datetime
from loguru import logger

# Add project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import init_db, get_db_session, Stock


# Nifty 50 symbols (inner ring of Nifty 500)
NIFTY_50 = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BPCL", "BHARTIARTL",
    "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY",
    "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
    "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC",
    "INDUSINDBK", "INFY", "JSWSTEEL", "KOTAKBANK", "LT",
    "M&M", "MARUTI", "NTPC", "NESTLEIND", "ONGC",
    "POWERGRID", "RELIANCE", "SBILIFE", "SHRIRAMFIN", "SBIN",
    "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL",
    "TECHM", "TITAN", "ULTRACEMCO", "WIPRO", "LTIM"
]

# Nifty Next 50 (part of Nifty 100)
NIFTY_NEXT_50 = [
    "ABB", "ADANIGREEN", "AMBUJACEM", "AUROPHARMA", "BANDHANBNK",
    "BANKBARODA", "BEL", "BERGEPAINT", "BOSCHLTD", "CANBK",
    "CHOLAFIN", "COLPAL", "DABUR", "DLF", "GAIL",
    "GODREJCP", "HAL", "HAVELLS", "HINDPETRO", "ICICIGI",
    "ICICIPRULI", "INDHOTEL", "INDUSTOWER", "IOC", "IRCTC",
    "JINDALSTEL", "JSWENERGY", "LICI", "LODHA", "LUPIN",
    "MARICO", "MOTHERSON", "NAUKRI", "NHPC", "PAGEIND",
    "PAYTM", "PFC", "PIDILITIND", "PNB", "POLYCAB",
    "RECLTD", "SBICARD", "SIEMENS", "SRF", "TORNTPHARM",
    "TRENT", "UNITDSPR", "VBL", "VEDL", "ZOMATO"
]

# Sample of additional Nifty 500 stocks (we'll load full list from CSV/API)
NIFTY_500_SAMPLE = [
    # IT
    ("COFORGE", "Coforge Ltd", "Information Technology", "IT Services"),
    ("PERSISTENT", "Persistent Systems Ltd", "Information Technology", "IT Services"),
    ("MPHASIS", "MphasiS Ltd", "Information Technology", "IT Services"),
    ("LTTS", "L&T Technology Services", "Information Technology", "IT Services"),
    
    # Banking
    ("AUBANK", "AU Small Finance Bank", "Financial Services", "Banks"),
    ("FEDERALBNK", "Federal Bank Ltd", "Financial Services", "Banks"),
    ("IDFCFIRSTB", "IDFC First Bank Ltd", "Financial Services", "Banks"),
    ("RBLBANK", "RBL Bank Ltd", "Financial Services", "Banks"),
    
    # Pharma
    ("ALKEM", "Alkem Laboratories Ltd", "Healthcare", "Pharmaceuticals"),
    ("BIOCON", "Biocon Ltd", "Healthcare", "Pharmaceuticals"),
    ("IPCALAB", "Ipca Laboratories Ltd", "Healthcare", "Pharmaceuticals"),
    ("LAURUSLABS", "Laurus Labs Ltd", "Healthcare", "Pharmaceuticals"),
    
    # Auto
    ("ASHOKLEY", "Ashok Leyland Ltd", "Automobile", "Commercial Vehicles"),
    ("BALKRISIND", "Balkrishna Industries", "Automobile", "Tyres"),
    ("BHARATFORG", "Bharat Forge Ltd", "Automobile", "Auto Components"),
    ("TVSMOTOR", "TVS Motor Company Ltd", "Automobile", "Two Wheelers"),
    
    # Metals
    ("NATIONALUM", "National Aluminium Co", "Metals & Mining", "Aluminium"),
    ("NMDC", "NMDC Ltd", "Metals & Mining", "Iron Ore"),
    ("SAIL", "Steel Authority of India", "Metals & Mining", "Steel"),
    ("HINDZINC", "Hindustan Zinc Ltd", "Metals & Mining", "Zinc"),
    
    # FMCG
    ("EMAMILTD", "Emami Ltd", "FMCG", "Personal Care"),
    ("GODREJIND", "Godrej Industries Ltd", "FMCG", "Diversified"),
    ("TATAPOWER", "Tata Power Company Ltd", "Power", "Power Generation"),
    ("TORNTPOWER", "Torrent Power Ltd", "Power", "Power Generation"),
    
    # Realty
    ("GODREJPROP", "Godrej Properties Ltd", "Realty", "Real Estate"),
    ("OBEROIRLTY", "Oberoi Realty Ltd", "Realty", "Real Estate"),
    ("PRESTIGE", "Prestige Estates Projects", "Realty", "Real Estate"),
    ("BRIGADE", "Brigade Enterprises Ltd", "Realty", "Real Estate"),
    
    # Capital Goods
    ("CUMMINSIND", "Cummins India Ltd", "Capital Goods", "Industrial Machinery"),
    ("THERMAX", "Thermax Ltd", "Capital Goods", "Industrial Machinery"),
    ("VOLTAS", "Voltas Ltd", "Capital Goods", "Consumer Durables"),
    ("BLUESTARCO", "Blue Star Ltd", "Capital Goods", "Consumer Durables"),
    
    # Chemicals
    ("AARTIIND", "Aarti Industries Ltd", "Chemicals", "Specialty Chemicals"),
    ("DEEPAKNTR", "Deepak Nitrite Ltd", "Chemicals", "Specialty Chemicals"),
    ("PIIND", "PI Industries Ltd", "Chemicals", "Agrochemicals"),
    ("UPL", "UPL Ltd", "Chemicals", "Agrochemicals"),
    
    # Consumer
    ("DMART", "Avenue Supermarts Ltd", "Consumer Services", "Retail"),
    ("JUBLFOOD", "Jubilant Foodworks Ltd", "Consumer Services", "Restaurants"),
    ("NYKAA", "FSN E-Commerce Ventures", "Consumer Services", "E-Commerce"),
    ("POLICYBZR", "PB Fintech Ltd", "Financial Services", "Insurance"),
    
    # ETFs/Index
    ("NIFTYBEES", "Nippon India ETF Nifty BeES", "ETF", "Index ETF"),
    ("BANKBEES", "Nippon India ETF Bank BeES", "ETF", "Sector ETF"),
    ("GOLDBEES", "Nippon India ETF Gold BeES", "ETF", "Commodity ETF"),
]

# Sector mapping for Nifty 50/100
STOCK_INFO = {
    # Nifty 50 with sectors
    "RELIANCE": ("Reliance Industries Ltd", "Oil & Gas", "Integrated Oil & Gas"),
    "TCS": ("Tata Consultancy Services", "Information Technology", "IT Services"),
    "HDFCBANK": ("HDFC Bank Ltd", "Financial Services", "Banks"),
    "INFY": ("Infosys Ltd", "Information Technology", "IT Services"),
    "ICICIBANK": ("ICICI Bank Ltd", "Financial Services", "Banks"),
    "HINDUNILVR": ("Hindustan Unilever Ltd", "FMCG", "Personal Care"),
    "ITC": ("ITC Ltd", "FMCG", "Tobacco & FMCG"),
    "SBIN": ("State Bank of India", "Financial Services", "Banks"),
    "BHARTIARTL": ("Bharti Airtel Ltd", "Telecommunication", "Telecom Services"),
    "KOTAKBANK": ("Kotak Mahindra Bank Ltd", "Financial Services", "Banks"),
    "LT": ("Larsen & Toubro Ltd", "Capital Goods", "Construction"),
    "AXISBANK": ("Axis Bank Ltd", "Financial Services", "Banks"),
    "ASIANPAINT": ("Asian Paints Ltd", "Consumer Durables", "Paints"),
    "MARUTI": ("Maruti Suzuki India Ltd", "Automobile", "Passenger Cars"),
    "BAJFINANCE": ("Bajaj Finance Ltd", "Financial Services", "NBFC"),
    "TITAN": ("Titan Company Ltd", "Consumer Durables", "Jewellery"),
    "SUNPHARMA": ("Sun Pharmaceutical Industries", "Healthcare", "Pharmaceuticals"),
    "HCLTECH": ("HCL Technologies Ltd", "Information Technology", "IT Services"),
    "WIPRO": ("Wipro Ltd", "Information Technology", "IT Services"),
    "NTPC": ("NTPC Ltd", "Power", "Power Generation"),
    "ULTRACEMCO": ("UltraTech Cement Ltd", "Construction Materials", "Cement"),
    "POWERGRID": ("Power Grid Corporation", "Power", "Power Transmission"),
    "NESTLEIND": ("Nestle India Ltd", "FMCG", "Food Products"),
    "ONGC": ("Oil & Natural Gas Corporation", "Oil & Gas", "Exploration"),
    "TATAMOTORS": ("Tata Motors Ltd", "Automobile", "Commercial Vehicles"),
    "M&M": ("Mahindra & Mahindra Ltd", "Automobile", "Farm Equipment"),
    "TATASTEEL": ("Tata Steel Ltd", "Metals & Mining", "Steel"),
    "ADANIENT": ("Adani Enterprises Ltd", "Diversified", "Trading"),
    "ADANIPORTS": ("Adani Ports and SEZ Ltd", "Services", "Port Services"),
    "COALINDIA": ("Coal India Ltd", "Metals & Mining", "Coal"),
    "BAJAJFINSV": ("Bajaj Finserv Ltd", "Financial Services", "Holding Company"),
    "BAJAJ-AUTO": ("Bajaj Auto Ltd", "Automobile", "Two Wheelers"),
    "JSWSTEEL": ("JSW Steel Ltd", "Metals & Mining", "Steel"),
    "GRASIM": ("Grasim Industries Ltd", "Construction Materials", "Cement"),
    "TECHM": ("Tech Mahindra Ltd", "Information Technology", "IT Services"),
    "INDUSINDBK": ("IndusInd Bank Ltd", "Financial Services", "Banks"),
    "HINDALCO": ("Hindalco Industries Ltd", "Metals & Mining", "Aluminium"),
    "DRREDDY": ("Dr. Reddy's Laboratories", "Healthcare", "Pharmaceuticals"),
    "CIPLA": ("Cipla Ltd", "Healthcare", "Pharmaceuticals"),
    "DIVISLAB": ("Divi's Laboratories Ltd", "Healthcare", "Pharmaceuticals"),
    "BRITANNIA": ("Britannia Industries Ltd", "FMCG", "Food Products"),
    "BPCL": ("Bharat Petroleum Corporation", "Oil & Gas", "Refining"),
    "EICHERMOT": ("Eicher Motors Ltd", "Automobile", "Two Wheelers"),
    "APOLLOHOSP": ("Apollo Hospitals Enterprise", "Healthcare", "Hospitals"),
    "HEROMOTOCO": ("Hero MotoCorp Ltd", "Automobile", "Two Wheelers"),
    "TATACONSUM": ("Tata Consumer Products Ltd", "FMCG", "Food Products"),
    "HDFCLIFE": ("HDFC Life Insurance Co", "Financial Services", "Insurance"),
    "SBILIFE": ("SBI Life Insurance Co", "Financial Services", "Insurance"),
    "SHRIRAMFIN": ("Shriram Finance Ltd", "Financial Services", "NBFC"),
    "LTIM": ("LTIMindtree Ltd", "Information Technology", "IT Services"),
}


def load_stocks():
    """Load Nifty 500 stocks into database."""
    init_db()
    
    with get_db_session() as db:
        # Check if already loaded
        existing = db.query(Stock).count()
        if existing > 0:
            logger.info(f"Stocks table already has {existing} entries. Skipping load.")
            return existing
        
        loaded = 0
        
        # Load Nifty 50
        for symbol in NIFTY_50:
            info = STOCK_INFO.get(symbol, (symbol, "Unknown", "Unknown"))
            stock = Stock(
                symbol=symbol,
                name=info[0],
                sector=info[1],
                industry=info[2],
                is_nifty50=True,
                is_nifty100=True,
                is_nifty200=True,
                is_nifty500=True
            )
            db.add(stock)
            loaded += 1
        
        # Load Nifty Next 50
        for symbol in NIFTY_NEXT_50:
            if db.query(Stock).filter_by(symbol=symbol).first():
                continue
            stock = Stock(
                symbol=symbol,
                name=symbol,  # Will update with proper names later
                is_nifty50=False,
                is_nifty100=True,
                is_nifty200=True,
                is_nifty500=True
            )
            db.add(stock)
            loaded += 1
        
        # Load additional Nifty 500 sample
        for item in NIFTY_500_SAMPLE:
            symbol, name, sector, industry = item
            if db.query(Stock).filter_by(symbol=symbol).first():
                continue
            stock = Stock(
                symbol=symbol,
                name=name,
                sector=sector,
                industry=industry,
                is_nifty50=False,
                is_nifty100=False,
                is_nifty200=False,
                is_nifty500=True
            )
            db.add(stock)
            loaded += 1
        
        logger.success(f"Loaded {loaded} stocks into database")
        return loaded


def get_all_symbols(index: str = "nifty500") -> list:
    """Get list of symbols for an index."""
    with get_db_session() as db:
        if index == "nifty50":
            stocks = db.query(Stock).filter_by(is_nifty50=True, is_active=True).all()
        elif index == "nifty100":
            stocks = db.query(Stock).filter_by(is_nifty100=True, is_active=True).all()
        elif index == "nifty500":
            stocks = db.query(Stock).filter_by(is_nifty500=True, is_active=True).all()
        else:
            stocks = db.query(Stock).filter_by(is_active=True).all()
        
        return [s.symbol for s in stocks]


def get_stocks_by_sector(sector: str) -> list:
    """Get stocks by sector."""
    with get_db_session() as db:
        stocks = db.query(Stock).filter(
            Stock.sector.ilike(f"%{sector}%"),
            Stock.is_active == True
        ).all()
        return [{"symbol": s.symbol, "name": s.name, "industry": s.industry} for s in stocks]


def search_stocks(query: str) -> list:
    """Search stocks by symbol or name."""
    with get_db_session() as db:
        stocks = db.query(Stock).filter(
            (Stock.symbol.ilike(f"%{query}%")) | 
            (Stock.name.ilike(f"%{query}%"))
        ).limit(20).all()
        return [{"symbol": s.symbol, "name": s.name, "sector": s.sector} for s in stocks]


if __name__ == "__main__":
    print("Loading Nifty 500 stocks...")
    count = load_stocks()
    print(f"Total stocks in database: {count}")
    
    print("\nNifty 50 symbols:")
    print(get_all_symbols("nifty50"))
    
    print("\nIT sector stocks:")
    print(get_stocks_by_sector("Information Technology"))
    
    print("\nSearch 'TATA':")
    print(search_stocks("TATA"))
