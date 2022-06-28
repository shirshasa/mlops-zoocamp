import pickle
import pandas as pd

from typing import Union


def read_data(file_path, categorical=None, target_name='duration'):
    """Load data from disk and preprocess for training."""
    categorical = categorical or ['PUlocationID', 'DOlocationID']
    df = pd.read_parquet(file_path)
    # Create target column and filter outliers
    df[target_name] = df.dropOff_datetime - df.pickup_datetime
    df[target_name] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return df


def prepare_features(df, dv):
    categorical = ['PUlocationID', 'DOlocationID']
    dicts = df[categorical].to_dict(orient='records')
    X = dv.transform(dicts)
    return X


def read_model(filename):
    with open(filename, 'rb') as f_in:
        dv, lr = pickle.load(f_in)
    return dv, lr
