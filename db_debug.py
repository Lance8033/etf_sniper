import sys
import io
from core.database import supabase
import pandas as pd

# Force UTF-8 for stdout
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def check_db():
    print("--- etf_config ---")
    res = supabase.table('etf_config').select('*').execute()
    df = pd.DataFrame(res.data)
    if not df.empty:
        print(df.to_string())
    else:
        print("Empty etf_config")

    print("\n--- etf_holdings_history (recent) ---")
    res = supabase.table('etf_holdings_history').select('*').order('date', desc=True).limit(10).execute()
    df_h = pd.DataFrame(res.data)
    if not df_h.empty:
        print(df_h.to_string())
    else:
        print("Empty history")

if __name__ == "__main__":
    check_db()
