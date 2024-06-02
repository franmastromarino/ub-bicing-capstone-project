import mlflow
# Predict on a Pandas DataFrame.
import pandas as pd

from ml_flow_utils import configure_databricks, setup_mlflow

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






loaded_model.predict(pd.DataFrame(data))