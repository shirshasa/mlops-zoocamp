import os
import pickle

import mlflow
from hyperopt import space_eval
from mlflow.entities import ViewType
from mlflow.tracking import MlflowClient
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

from hpo import SPACE
from config import Configs

config = Configs.from_file()

HPO_EXPERIMENT_NAME = config.experiment['hyper_params_rf_train']
TESTSET_EXPERIMENT_NAME = config.experiment['best_models_run']
MODEL_NAME = "green-taxi-regressor"

mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.set_experiment(TESTSET_EXPERIMENT_NAME)
mlflow.sklearn.autolog()


def load_pickle(filename):
    with open(filename, "rb") as f_in:
        return pickle.load(f_in)


def train_and_log_model(data_path, params):
    X_train, y_train = load_pickle(os.path.join(os.path.dirname(__file__), data_path, "train.pkl"))
    X_valid, y_valid = load_pickle(os.path.join(os.path.dirname(__file__), data_path, "valid.pkl"))
    X_test, y_test = load_pickle(os.path.join(os.path.dirname(__file__), data_path, "test.pkl"))

    with mlflow.start_run():
        params = space_eval(SPACE, params)
        rf = RandomForestRegressor(**params)
        rf.fit(X_train, y_train)

        # evaluate model on the validation and test sets
        valid_rmse = mean_squared_error(y_valid, rf.predict(X_valid), squared=False)
        mlflow.log_metric("valid_rmse", valid_rmse)
        test_rmse = mean_squared_error(y_test, rf.predict(X_test), squared=False)
        mlflow.log_metric("test_rmse", test_rmse)


def run(log_top=5):

    client = MlflowClient()

    # retrieve the top_n model runs and log the models to MLflow
    experiment = client.get_experiment_by_name(HPO_EXPERIMENT_NAME)
    runs = client.search_runs(
        experiment_ids=experiment.experiment_id,
        run_view_type=ViewType.ACTIVE_ONLY,
        max_results=log_top,
        order_by=["metrics.rmse ASC"]
    )
    for run in runs:
        train_and_log_model(data_path=config.path['artifacts_path'], params=run.data.params)

    # select the model with the lowest test RMSE
    experiment = client.get_experiment_by_name(TESTSET_EXPERIMENT_NAME)
    best_run = client.search_runs(
        experiment_ids=experiment.experiment_id,
        run_view_type=ViewType.ACTIVE_ONLY,
        max_results=log_top,
        order_by=["metrics.test_rmse ASC"]
    )[0]

    run_id = best_run.info.run_id
    model_uri = f"runs:/{run_id}/models"
    mlflow.register_model(model_uri=model_uri, name=MODEL_NAME)


if __name__ == '__main__':
    run()
