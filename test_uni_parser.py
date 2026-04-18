import sys
import io
from parsers.unipresident import UniPresidentParser

# Force UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def test():
    p = UniPresidentParser("00981A", "統一")
    date = "2026-04-17"
    print(f"Fetching 00981A for {date}...")
    df = p.fetch_data(date)
    if not df.empty:
        print(f"SUCCESS! Got {len(df)} rows.")
        print(df.head(10).to_string())
    else:
        print("Empty result. API might be returning nothing for this date or code.")

if __name__ == "__main__":
    test()
