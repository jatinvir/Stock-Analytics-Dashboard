import argparse
from dotenv import load_dotenv

load_dotenv(".env.dev")

from ingest import ingest_one_symbol, ingest_all_symbols

def main():
    argument = argparse.ArgumentParser(description="Ingest stock data into the database")

    ingest_group = argument.add_mutually_exclusive_group(required=True)
    ingest_group.add_argument("--symbol", type=str, help="Ingest data for a specific stock symbol")
    ingest_group.add_argument("--all", action="store_true", help="Ingest data for all stock symbols")

    argument.add_argument("--days", type=int, default=20, help="Number of days of historical data to ingest (default: 20)")

    args = argument.parse_args()

    if args.symbol:
        result = ingest_one_symbol(args.symbol, lookback_days=args.days)
    else:
        result = ingest_all_symbols(lookback_days=args.days)
    
    print(result)
    return result

if __name__ == "__main__":
    main()