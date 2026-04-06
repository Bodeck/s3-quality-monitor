import pandas as pd
from monitor.validators import NullRatioCheck, SchemaCheck, RowCountCheck

def test_null_ratio_check_passes():
    df = pd.DataFrame({"name": ["john", "rachel", "alice", "mark"], "level": [1, 1, 4, 9] })
    validator = NullRatioCheck(.1)
    results = validator.validate(df)
    assert results.passed

def test_null_ratio_check_fails():
    df = pd.DataFrame({"name": ["john", None, "alice", "mark"], "level": [1, 1, 4, 9] })
    validator = NullRatioCheck(.1)
    results = validator.validate(df)

    assert not results.passed
    assert results.details

def test_null_ratio_check_missing_column():
    df = pd.DataFrame({"name": ["john", None, "alice", "mark"], "level": [1, 1, 4, 9] })
    validator = NullRatioCheck(.1, column="surname")
    results = validator.validate(df)
    
    assert not results.passed
    assert results.details

def test_null_ratio_check_column_level_failed():
    df = pd.DataFrame({"name": ["john", None, "alice", "mark"], "level": [1, 1, 4, 9] })
    validator = NullRatioCheck(.1, column="name")
    results = validator.validate(df)
    
    assert not results.passed
    assert results.details

def test_null_ratio_check_column_level_passes():
    df = pd.DataFrame({"name": ["john", "lucien"], "level": [None, None]})
    validator = NullRatioCheck(.1, "name")
    
    assert validator.validate(df).passed

def test_schema_check_passes():
    df = pd.DataFrame({"name": ["john", None, "alice", "mark"], "level": [1, 1, 4, 8] })
    validator = SchemaCheck(["name", "level"])
    results = validator.validate(df)

    assert results.passed

def test_schema_check_missing_columns():
    df = pd.DataFrame({"name": ["john", None, "alice", "mark"], "level": [1, 1, 4, 9] })
    validator = SchemaCheck(["name", "level", "skills"])
    results = validator.validate(df)

    assert not results.passed
    assert results.details

def test_row_count_check_passes():
    df = pd.DataFrame({"name": ["john", None, "alice", "mark"], "level": [1, 1, 4, 9] })
    validator = RowCountCheck(4)
    results = validator.validate(df)

    assert results.passed

def test_row_count_check_fails():
    df = pd.DataFrame({"name": ["john", None, "alice", "mark"], "level": [1, 1, 4, 9] })
    validator = RowCountCheck(5)
    results = validator.validate(df)

    assert not results.passed
    assert results.details

def test_row_count_check_boundary():
    df = pd.DataFrame({"id": range(5)})
    exact_check = RowCountCheck(5).validate(df)
    above_check = RowCountCheck(6).validate(df)

    assert exact_check.passed
    assert not above_check.passed