# 
# Used in DAG's local development
# 
# Usage:
# with DAG(...) as dag:
#     ...
# 
# if __name__ == "__main__":
#     from include.dag_local_config_test import local_test_configs
# 
#     dag.test(**local_test_configs)
# 

from pendulum import datetime

local_test_configs = {
    "execution_date": datetime(2020, 1, 1),
    "conn_file_path": "include/connections.yml",
    "variable_file_path": "include/variables.yml"
}