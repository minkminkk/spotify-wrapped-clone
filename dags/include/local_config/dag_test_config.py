# 
# Used in DAG's local development
# 
# Usage:
# with DAG(...) as dag:
#     ...
# 
# if __name__ == "__main__":
#     from include.dag_local_config_test import local_test_configs
#     dag.test(**local_test_configs)
# 

import os

include_dir = os.path.dirname(__file__)
local_test_configs = {
    "conn_file_path": os.path.join(include_dir, "connections.yml"),
    "variable_file_path": os.path.join(include_dir, "variables.yml")
}