from flask import Flask, render_template, request
from databricks.connect import DatabricksSession
import os

app = Flask(__name__)

# Retrieve Databricks connection details from environment variables
workspace_host = os.getenv("DATABRICKS_SERVER_HOST")
personal_access_token = os.getenv("DATABRICKS_TOKEN")
cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")

if not workspace_host or not personal_access_token or not cluster_id:
    raise EnvironmentError("Databricks connection error")

# Initialize DatabricksSession
try:
    spark = DatabricksSession.builder.remote(
        host=f"https://{workspace_host}",
        token=personal_access_token,
        cluster_id=cluster_id
    ).getOrCreate()
    print("Databricks session initialized successfully.")
except Exception as e:
    print("Failed to initialize Databricks session.")
    print(e)
    raise

# Function to calculate attrition metrics for a department
def get_department_attrition(department):
    table_name = "ids706_data_engineering.hr_analytics.employee_attrition_data_filtered"
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
    except Exception as e:
        raise ValueError(f"Table '{table_name}' does not exist.") from e

    try:
        df = spark.sql(f"SELECT * FROM {table_name} WHERE Department = '{department}'")
        total_employees = df.count()
        if total_employees == 0:
            return {"total_employees": 0, "employees_left": 0, "attrition_rate": 0.0}
        employees_left = df.filter(df.Attrition == "Yes").count()
        attrition_rate = (employees_left / total_employees * 100)
        return {
            "total_employees": total_employees,
            "employees_left": employees_left,
            "attrition_rate": round(attrition_rate, 2)
        }
    except Exception as e:
        raise RuntimeError("Failed to execute SQL query or calculate metrics.") from e

@app.route("/", methods=["GET", "POST"])
def index():
    departments = ["Sales", "Research & Development", "Human Resources"]
    selected_department = None
    metrics = None
    try:
        if request.method == "POST":
            selected_department = request.form.get("dropdown")
            if selected_department:
                metrics = get_department_attrition(selected_department)
        return render_template(
            "index.html",
            departments=departments,
            selected_department=selected_department,
            metrics=metrics
        )
    except Exception as e:
        return f"An error occurred: {e}", 500

if __name__ == "__main__":
    debug_mode = os.getenv("FLASK_DEBUG", "False").lower() == "true"
    app.run(host="0.0.0.0", port=80, debug=debug_mode)