import requests
from config import settings


class AirflowClient:
    def __init__(self):
        self.base_url = settings.AIRFLOW_BASE_URL
        self.auth = (settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD)

    def _get(self, path: str) -> dict:
        response = requests.get(f"{self.base_url}/api/v1{path}", auth=self.auth)
        response.raise_for_status()
        return response.json()

    def _post(self, path: str, payload: dict) -> dict:
        response = requests.post(
            f"{self.base_url}/api/v1{path}",
            json=payload,
            auth=self.auth,
        )
        response.raise_for_status()
        return response.json()

    def trigger_dag(self, dag_id: str, conf: dict = {}) -> dict:
        """Trigger a DAG run via Airflow REST API."""
        return self._post(f"/dags/{dag_id}/dagRuns", {"conf": conf})

    def list_dags(self) -> dict:
        """List all DAGs."""
        return self._get("/dags")

    def get_dag_runs(self, dag_id: str) -> dict:
        """Get all runs for a DAG."""
        return self._get(f"/dags/{dag_id}/dagRuns")

    def get_dag_run_status(self, dag_id: str, dag_run_id: str) -> dict:
        """Get status of a specific DAG run."""
        return self._get(f"/dags/{dag_id}/dagRuns/{dag_run_id}")