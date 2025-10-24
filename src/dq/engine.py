import json, re
from pathlib import Path
import pandas as pd

class DQResult:
    def __init__(self):
        self.violations = []
        self.summary = {}
    def add(self, rule, count):
        self.summary[rule] = self.summary.get(rule, 0) + count
    def add_rows(self, rule, rows):
        self.violations.append({"rule": rule, "rows": rows})
    def to_json(self):
        return json.dumps({"summary": self.summary, "violations": self.violations}, indent=2)

def load_rules(path: str) -> dict:
    return json.loads(Path(path).read_text())

def _ensure_df(obj):
    return obj if isinstance(obj, pd.DataFrame) else pd.DataFrame(obj)

def validate_not_null(df, columns):
    df = _ensure_df(df)
    mask = None
    for c in columns:
        m = df[c].isna() | (df[c].astype(str).str.len()==0)
        mask = m if mask is None else (mask | m)
    return df[mask] if mask is not None else df.iloc[0:0]

def validate_unique(df, columns):
    df = _ensure_df(df)
    dups = df[df.duplicated(subset=columns, keep=False)].sort_values(columns)
    return dups

def validate_regex(df, column, pattern):
    df = _ensure_df(df)
    bad = ~df[column].astype(str).str.match(pattern, na=False)
    return df[bad]

def validate_range(df, column, min_v=None, max_v=None):
    df = _ensure_df(df)
    s = pd.to_numeric(df[column], errors='coerce')
    bad = False
    if min_v is not None:
        bad = (s < min_v) | (s.isna())
    if max_v is not None:
        bad = bad | (s > max_v)
    return df[bad]

def validate_regex_date(df, column, pattern):
    return validate_regex(df, column, pattern)

def validate_domain(df, column, values):
    df = _ensure_df(df)
    bad = ~df[column].isin(values)
    return df[bad]

def validate_fk(df_child, df_parent, child_cols, parent_cols):
    df_child = _ensure_df(df_child)
    df_parent = _ensure_df(df_parent)
    merged = df_child.merge(df_parent[parent_cols].drop_duplicates(), left_on=child_cols, right_on=parent_cols, how='left', indicator=True)
    return merged[merged['_merge'] == 'left_only'][df_child.columns]

def run_rules(df_map, rules_cfg):
    res = DQResult()
    table = rules_cfg['table']
    df = df_map[table]
    for rule in rules_cfg.get('rules', []):
        rtype = rule['type']
        if rtype == 'not_null':
            bad = validate_not_null(df, rule['columns'])
            res.add('not_null', len(bad))
            if len(bad): res.add_rows('not_null', bad.head(100).to_dict(orient='records'))
        elif rtype == 'unique':
            bad = validate_unique(df, rule['columns'])
            res.add('unique', len(bad))
            if len(bad): res.add_rows('unique', bad.head(100).to_dict(orient='records'))
        elif rtype == 'regex':
            bad = validate_regex(df, rule['column'], rule['pattern'])
            res.add('regex', len(bad))
            if len(bad): res.add_rows('regex', bad.head(100).to_dict(orient='records'))
        elif rtype == 'range':
            bad = validate_range(df, rule['column'], rule.get('min'), rule.get('max'))
            res.add('range', len(bad))
            if len(bad): res.add_rows('range', bad.head(100).to_dict(orient='records'))
        elif rtype == 'regex_date':
            bad = validate_regex_date(df, rule['column'], rule['pattern'])
            res.add('regex_date', len(bad))
            if len(bad): res.add_rows('regex_date', bad.head(100).to_dict(orient='records'))
        elif rtype == 'domain':
            bad = validate_domain(df, rule['column'], rule['values'])
            res.add('domain', len(bad))
            if len(bad): res.add_rows('domain', bad.head(100).to_dict(orient='records'))
    # foreign keys
    for fk in rules_cfg.get('foreign_keys', []):
        child_cols = fk['columns']
        ref = fk['ref_table']
        ref_cols = fk['ref_columns']
        bad = validate_fk(df, df_map[ref], child_cols, ref_cols)
        res.add('foreign_key', len(bad))
        if len(bad): res.add_rows('foreign_key', bad.head(100).to_dict(orient='records'))
    return res
