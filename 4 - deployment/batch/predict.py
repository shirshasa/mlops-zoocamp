import os

from utils import read_data, read_model, prepare_features
from sklearn.metrics import mean_squared_error
from dotenv import load_dotenv


def make_predictions(df, dv, lr):
    X = prepare_features(df, dv)
    predictions = lr.predict(X)
    return predictions


def predictions2file(year, month, output_file='predictions.parquet'):
    load_dotenv()
    target = 'duration'
    url = f'https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_{year}-{month:02}.parquet'
    df = read_data(url, target_name=target)
    dv, lr = read_model(os.getenv("MODEL_PATH"))
    predictions = make_predictions(df, dv, lr)

    print(f'RMSE: {mean_squared_error(df[target], predictions, squared=False)}')
    print(f'Mean duration: {predictions.mean()}')

    df['predictions'] = predictions
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    df_result = df[['ride_id', 'predictions']]

    df_result.to_parquet(
        output_file,
        engine='pyarrow',
        compression=None,
        index=False
    )


if __name__ == "__main__":
    predictions2file(year=2021, month=4, output_file='../predictions.parquet')
