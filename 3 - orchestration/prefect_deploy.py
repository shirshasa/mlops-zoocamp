import pickle
import pandas as pd
from datetime import datetime

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

from prefect import flow, task, get_run_logger
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule
from prefect.flow_runners import SubprocessFlowRunner


def read_data(path):
    df = pd.read_parquet(path)
    return df


def dump_pickle(obj, filename):
    with open(filename, "wb") as f_out:
        return pickle.dump(obj, f_out)


@task
def prepare_features(df, categorical, train=True):
    logger = get_run_logger()

    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    mean_duration = df.duration.mean()
    if train:
        logger.info(f"The mean duration of training is {mean_duration}")
    else:
        logger.info(f"The mean duration of validation is {mean_duration}")

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return df


@task
def train_model(df, categorical):
    logger = get_run_logger()

    train_dicts = df[categorical].to_dict(orient='records')
    dv = DictVectorizer()
    X_train = dv.fit_transform(train_dicts)
    y_train = df.duration.values

    logger.info(f"The shape of X_train is {X_train.shape}")
    logger.info(f"The DictVectorizer has {len(dv.feature_names_)} features")

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    y_pred = lr.predict(X_train)
    mse = mean_squared_error(y_train, y_pred, squared=False)
    logger.info(f"The MSE of training is: {mse}")
    return lr, dv


@task
def run_model(df, categorical, dv, lr):
    logger = get_run_logger()

    val_dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(val_dicts)
    y_pred = lr.predict(X_val)
    y_val = df.duration.values

    mse = mean_squared_error(y_val, y_pred, squared=False)
    logger.info(f"The MSE of validation is: {mse}")
    return


@task
def get_paths(date: datetime):
    """
    Func returns train, valid dataset file paths for training process.
    The training data is 2 month back data from the date, the validation data is the previous month data.
    """
    train_year = date.year if (date.month - 2) > 0 else (date.year - 1)
    train_month = (12 + (date.month - 2)) % 12 or 12

    val_year = date.year if (date.month - 1) > 0 else (date.year - 1)
    val_month = (12 + (date.month - 1)) % 12 or 12

    return f'./data/fhv_tripdata_{train_year}-{train_month:02}.parquet', \
           f'./data/fhv_tripdata_{val_year}-{val_month:02}.parquet'


@flow
def main(date: str):
    categorical = ['PUlocationID', 'DOlocationID']
    date = datetime.fromisoformat(date) if date else datetime.now()
    train_path, val_path = get_paths(date).result()
    df_train = read_data(train_path)
    df_train_processed = prepare_features(df_train, categorical).result()

    df_val = read_data(val_path)
    df_val_processed = prepare_features(df_val, categorical, False).result()

    # train the model
    lr, dv = train_model(df_train_processed, categorical).result()
    dump_pickle(lr, f'model-{date.date()}.pkl')
    dump_pickle(dv, f'dv-{date.date()}.pkl')

    run_model(df_val_processed, categorical, dv, lr)


DeploymentSpec(
    flow=main,
    name="model_training",
    schedule=CronSchedule(cron="0 9 15 * *"),
    flow_runner=SubprocessFlowRunner(),
    tags=["ml"]
)
