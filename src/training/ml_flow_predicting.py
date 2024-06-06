import mlflow
# Predict on a Pandas DataFrame.
import pandas as pd
import os
from sklearn.preprocessing import OneHotEncoder
from sklearn.discriminant_analysis import StandardScaler



from mlflow_utils import configure_databricks, setup_mlflow

#This is not used, could be used for the subprocess
my_username = "ulisesreytorne@gmail.com"
my_password = "Loscinco5!"
print("El usuario y la contraseña son:")
print(my_username)
print(my_password)

configure_databricks(my_username, my_password)

setup_mlflow(my_username)

# Load model as a PyFuncModel.

logged_model = 'runs:/3ad33955f8dc4d01bdb52904cfedb79a/model'

loaded_model = mlflow.pyfunc.load_model(logged_model)

print(os.path.abspath(os.getcwd()))
full_df = pd.read_csv("./data/processed/groupby/stations_final_2024.csv")

features = ["weekday", "month", "hour", "post_code", "ctx-4", "ctx-3", "ctx-2", "ctx-1", "altitude", "laboral_day"]

categorical_features = ["weekday", "month", "hour", "post_code"]

y = full_df['percentage_docks_available']

df = full_df[features]

if categorical_features:
    one_hot_encoder = OneHotEncoder(sparse_output=False)
    one_hot_variables = one_hot_encoder.fit_transform(df[categorical_features])
    encoded_df = pd.DataFrame(one_hot_variables, columns=one_hot_encoder.get_feature_names_out())
    df = pd.concat([df, encoded_df], axis=1)
    df.drop(columns=categorical_features, inplace=True)

    X_test = df

scaler = StandardScaler()
X_test = scaler.fit_transform(X_test)




y_pred = loaded_model.predict(X_test)

prediction_df = pd.DataFrame(y_pred, columns=["percentage_docks_available"])
prediction_df.index.name="index"
prediction_df.to_csv("./data/processed/groupby/pred_stations_final_2024.csv")