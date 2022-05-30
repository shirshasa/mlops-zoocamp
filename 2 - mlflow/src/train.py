import os
import pickle
import mlflow

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

from config import Configs

config = Configs.from_file()


def load_pickle(filename: str):
    with open(filename, "rb") as f_in:
        return pickle.load(f_in)


def run():
    mlflow.set_tracking_uri(config.path['tracking_uri'])
    mlflow.set_experiment(config.experiment['base_train'])

    X_train, y_train = load_pickle(os.path.join(config.path['artifacts_path'], "train.pkl"))
    X_valid, y_valid = load_pickle(os.path.join(config.path['artifacts_path'], "valid.pkl"))

    with mlflow.start_run():
        mlflow.autolog()
        rf = RandomForestRegressor(max_depth=10, random_state=0)
        rf.fit(X_train, y_train)
        y_pred = rf.predict(X_valid)
        rmse = mean_squared_error(y_valid, y_pred, squared=False)


if __name__ == '__main__':
    run()

