import pandas as pd
from src.dq.engine import validate_not_null, validate_unique, validate_regex, validate_range, validate_domain

def test_not_null():
    df = pd.DataFrame({'a':[1,None,'']})
    bad = validate_not_null(df, ['a'])
    assert len(bad)==2

def test_unique():
    df = pd.DataFrame({'id':[1,1,2]})
    bad = validate_unique(df, ['id'])
    assert len(bad)==2

def test_regex():
    df = pd.DataFrame({'e':['a@b.com','not-an-email']})
    bad = validate_regex(df, 'e', r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
    assert len(bad)==1

def test_range():
    df = pd.DataFrame({'x':[0,5,1000]})
    bad = validate_range(df, 'x', 1, 500)
    assert set(bad['x']) == {0,1000}

def test_domain():
    df = pd.DataFrame({'c':['US','ZZ']})
    bad = validate_domain(df, 'c', ['US','CA'])
    assert len(bad)==1
