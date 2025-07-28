import pytest
import pandas as pd
from unittest.mock import MagicMock
from src import etl


def test_get_race_id_valid():
    key = 'some/prefix/23001A_Q1.csv'
    result = etl.get_race_id(key)
    assert result['event_id'] == '23001A'
    assert result['session_id'] == 'Q1'
    assert result['extention'] == 'csv'
    assert result['event_year'] == '23'
    assert result['event_num'] == '001'
    assert result['event_code'] == 'A'


def test_get_race_id_invalid_format():
    key = 'badfile.csv'
    with pytest.raises(ValueError):
        etl.get_race_id(key)


def test_validate_data_valid():
    df = pd.DataFrame({
        'timeUtc': ['2023-01-01T12:00:00Z'],
        'driverNumber': [44],
        'rpm': [12000],
        'speed': [300],
        'gear': [5],
        'throttle': [80],
        'brake': [0],
        'drs': [1]
    })
    result = etl.validate_data(df)
    assert result['is_valid']


def test_validate_data_missing_column():
    df = pd.DataFrame({
        'timeUtc': ['2023-01-01T12:00:00Z'],
        'driverNumber': [44],
        'rpm': [12000],
        'speed': [300],
        'gear': [5],
        'throttle': [80],
        'brake': [0]
        # 'drs' missing
    })
    result = etl.validate_data(df)
    assert not result['is_valid']
    assert any('drs' in err for err in result['errors'])


def test_transform_data_adds_metadata():
    df = pd.DataFrame({
        'timeUtc': ['2023-01-01T12:00:00Z'],
        'driverNumber': [44],
        'rpm': [12000],
        'speed': [300],
        'gear': [5],
        'throttle': [80],
        'brake': [0],
        'drs': [1]
    })
    meta = {
        'event_id': '23001A',
        'event_year': '23',
        'event_num': '001',
        'event_code': 'A',
        'session_id': 'Q1',
        'file_name_with_extention': '23001A_Q1.csv',
        'file_name': '23001A_Q1',
        'extention': 'csv'
    }
    df2 = etl.transform_data(df.copy(), meta)
    for col in ['event_id', 'event_year', 'event_num', 'event_code', 'session_id']:
        assert col in df2.columns


def test_transform_data_bad_metadata():
    df = pd.DataFrame({
        'timeUtc': ['2023-01-01T12:00:00Z'],
        'driverNumber': [44],
        'rpm': [12000],
        'speed': [300],
        'gear': [5],
        'throttle': [80],
        'brake': [0],
        'drs': [1]
    })
    meta = {'foo': 'bar'}  # Not enough keys
    with pytest.raises(ValueError):
        etl.transform_data(df, meta)


def test_read_file_no_such_key():
    s3 = MagicMock()
    s3.get_object.side_effect = Exception('NoSuchKey')
    with pytest.raises(Exception):
        etl.read_file('bucket', 'bad/key.csv', s3) 