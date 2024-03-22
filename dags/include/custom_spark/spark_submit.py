# 
# Quick workaround for SparkSubmitHook incorrectly resolving master URI
# for connection `spark://spark-master:7077` (got `spark-master:7077` instead)
# 

from typing import Any
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
from airflow.providers.apache.spark.operators.spark_submit \
    import SparkSubmitOperator


class CustomSparkSubmitHook(SparkSubmitHook):
    def _resolve_connection(self) -> dict[str, Any]:
        conn_data = super()._resolve_connection()
        
        # Work-around incorrect parsing for SparkSubmitHook
        if conn_data["master"] not in ("yarn", "local"):
            conn_data["master"] = "spark://" + conn_data["master"]

        return conn_data 


class CustomSparkSubmitOperator(SparkSubmitOperator):
    def _get_hook(self) -> CustomSparkSubmitHook:
        return CustomSparkSubmitHook(
            conf = self._conf,
            conn_id = self._conn_id,
            files = self._files,
            py_files = self._py_files,
            archives = self._archives,
            driver_class_path = self._driver_class_path,
            jars = self._jars,
            java_class = self._java_class,
            packages = self._packages,
            exclude_packages = self._exclude_packages,
            repositories = self._repositories,
            total_executor_cores = self._total_executor_cores,
            executor_cores = self._executor_cores,
            executor_memory = self._executor_memory,
            driver_memory = self._driver_memory,
            keytab = self._keytab,
            principal = self._principal,
            proxy_user = self._proxy_user,
            name = self._name,
            num_executors = self._num_executors,
            status_poll_interval = self._status_poll_interval,
            application_args = self._application_args,
            env_vars = self._env_vars,
            verbose = self._verbose,
            spark_binary = self._spark_binary,
            properties_file = self._properties_file,
            queue = self._queue,
            deploy_mode = self._deploy_mode,
            use_krb5ccache = self._use_krb5ccache,
        )   # arg list copied from API reference