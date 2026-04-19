import datetime
from parsers.mock_parser import MockHtmlTableParser, MockCsvDownloadParser
from parsers.unipresident import UniPresidentParser
from parsers.taishin import TaishinParser
from parsers.capital import CapitalParser
from core.database import supabase, check_db_connection, init_connection

# Parser Factory Mapping
PARSER_REGISTRY = {
    "網頁表格型 (HTML Table)": MockHtmlTableParser,
    "CSV下載型 (CSV Download)": MockCsvDownloadParser,
    "台新投信專用": TaishinParser,
    "群益投信專用": CapitalParser
}

def get_parser(parser_type: str, ticker: str, issuer: str):
    """獲取解析器實例處理路徑"""
    # 優先檢查是否有特定投信的專用解析器
    issuer_clean = issuer.strip()
    if issuer_clean == "統一":
        return UniPresidentParser(ticker, issuer)
    elif issuer_clean == "台新":
        return TaishinParser(ticker, issuer)
    elif issuer_clean == "群益":
        return CapitalParser(ticker, issuer)
        
    parser_class = PARSER_REGISTRY.get(parser_type)
    if not parser_class:
        raise ValueError(f"Unknown parser type: {parser_type}")
    return parser_class(ticker, issuer)

def get_next_trading_day(dt: datetime.date) -> datetime.date:
    """獲取下一個交易日 (跳過週末)"""
    next_day = dt + datetime.timedelta(days=1)
    while next_day.weekday() >= 5: # 週六(5), 週日(6)
        next_day += datetime.timedelta(days=1)
    return next_day

def execute_history_sync(ticker: str, issuer: str, parser_type: str, *args):
    """
    執行 ETF 歷史資料同步。
    邏輯：要獲得「T 日收盤持股」，必須抓取「T+1 日的 PCF」。
    """
    supabase = init_connection()
    if not supabase: return
    
    # 1. 清除該 ETF 的所有舊歷史資料，準備重新對齊
    try:
        supabase.table('etf_holdings_history').delete().eq('ticker', ticker).execute()
    except Exception:
        pass
    
    parser = get_parser(parser_type, ticker, issuer)
    today = datetime.date.today()
    
    # 抓取最近 14 個「潛在交易日」
    dates = [(today - datetime.timedelta(days=i)) for i in range(14, -1, -1)]
    
    for dt in dates:
        # A. 略過週末 (週六=5, 週日=6) 的「持股日」
        if dt.weekday() >= 5:
            continue
            
        holding_date_str = dt.strftime("%Y-%m-%d")
        
        # B. 核心邏輯修正：要抓取 holding_date_str 的成果，必須看下一個交易日的 PCF
        fetch_date = get_next_trading_day(dt)
        fetch_date_str = fetch_date.strftime("%Y-%m-%d")
        
        print(f"Syncing: Target {holding_date_str} (using PCF data from {fetch_date_str})")
        df = parser.fetch_data(fetch_date_str)
        
        # 存入資料庫時，我們將日期標記為「持股日」
        df['date'] = holding_date_str
        records = df.to_dict(orient='records')
        
        if records:
            try:
                supabase.table('etf_holdings_history').upsert(records).execute()
            except Exception as e:
                print(f"Error syncing {ticker} for {holding_date_str}: {e}")
