import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    files = ['green_tripdata_2020-10.csv.gz', 'green_tripdata_2020-11.csv.gz', 'green_tripdata_2020-12.csv.gz']
    # url_base = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green'
    url_base = 'http://172.26.255.122:8000'
    
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID':pd.Int64Dtype(),
        'store_and_fwd_flag':str,
        'PULocationID':pd.Int64Dtype(),
        'DOLocationID':pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra':float,
        'mta_tax':float,
        'tip_amount':float,
        'tolls_amount':float,
        'improvement_surcharge':float,
        'total_amount':float,
        'congestion_surcharge':float
    }
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    df = list()
    for file_name in files:
        url = f"{url_base}/{file_name}"
        print(f"Downloading {url}")
        _tmp = pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
        if len(df) == 0:
            df = _tmp
        else:
            df = pd.concat([df, _tmp])
        print("Download Done")

    return df
    # df = pd.DataFrame()
    # for url in urls:
    #     tmp_df = pd.read_csv(url, sep=',', compression='gzip')
    #     pd.concat(df, tmp_df)

    # # url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2021-07.csv.gz'

    # # return pd.read_csv(url, sep=',', compression='gzip')
    # return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
