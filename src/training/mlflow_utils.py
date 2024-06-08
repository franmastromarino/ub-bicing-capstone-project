import subprocess
import mlflow

# Define the command as a list of arguments
def configure_databricks(username, password):
    command = ["databricks", "configure", "--host", "https://community.cloud.databricks.com/"]
    # Use subprocess to execute the command with username and password
    process = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE)
    process.communicate(input=f"{username}\n{password}\n".encode())

# Set up MLflow tracking
def setup_mlflow(username):
    databricks_base_url = f'/Users/{username}'
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(f'{databricks_base_url}/bicing')