if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from datetime import datetime
import pandas as pd
import re

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    # print(data.head())

    # name = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

    to_rename = {
        'VendorID':'vendor_id',
        'RatecodeID': 'ratecode_id',
        'PULocationID':'pu_location_id',
        'DOLocationID':'do_location_id'
    }
    # for column in list(data):
    #     to_rename.update({column: re.sub(r'(?<!^)(?=[A-Z])', '_', column).lower()})

    data.rename(columns=to_rename, inplace=True)
    # data.drop(data.loc[(data['trip_distance']<=0 or data['passenger_count']<=0)].index, inplace=True)
    # data.drop(data.loc[data['passenger_count']<=0].index, inplace=True)
    # data = data.assign(lpep_pickup_date=pd.Series([x.date() for x in data['lpep_pickup_datetime']]))
    data = data[(data['passenger_count'] > 0) & (data['trip_distance']>0)]
    data = data.assign(lpep_pickup_date=data['lpep_pickup_datetime'].dt.date)
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """

    assert 'vendor_id' in list(output) \
    and not(output['passenger_count']==0).any() \
    and not(output['trip_distance']==0).any(), 'Test Failed'