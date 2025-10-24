import argparse, json
from pathlib import Path
import pandas as pd
from src.dq.engine import load_rules, run_rules

def load_partition(base_dir: Path, domain: str, ingest_date: str) -> pd.DataFrame:
    p = base_dir / 'sample_data' / 'raw' / domain / f'ingest_date={ingest_date}'
    frames = []
    for f in p.glob('*.csv'):
        frames.append(pd.read_csv(f))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', required=True, help='YYYY-MM-DD partition date')
    ap.add_argument('--out', default='reports')
    args = ap.parse_args()
    base = Path(__file__).parents[1]
    # load data
    df_customer = load_partition(base, 'customer', args.date)
    df_txn = load_partition(base, 'transactions', args.date)
    df_map = {'customer': df_customer, 'transactions': df_txn}
    # run rules
    cust_rules = load_rules(str(base / 'configs' / 'customer_rules.json'))
    txn_rules = load_rules(str(base / 'configs' / 'transactions_rules.json'))
    cust_res = run_rules(df_map, cust_rules)
    txn_res = run_rules(df_map, txn_rules)
    # write reports
    out_dir = base / args.out / args.date
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / 'customer.json').write_text(cust_res.to_json())
    (out_dir / 'transactions.json').write_text(txn_res.to_json())
    print(f'Wrote reports to {out_dir}')

if __name__ == '__main__':
    main()
