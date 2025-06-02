import requests
from databricks import sql
from google.auth.transport.requests import Request
from google.oauth2 import id_token
from typing import Any

class DatabricksSQLConnector:
    """
    Helper to get a Databricks SQL connection via GCP service account identity token.
    """

    def __init__(self, databricks_host: str, http_path: str, client_id: str, client_secret: str):
        self.databricks_host = databricks_host
        self.http_path = http_path
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_exchange_url = f"{self.databricks_host}/oidc/v1/token"

    def _get_google_id_token(self, audience: str = "https://accounts.google.com") -> str:
        """Fetch a GCP identity token for the service account."""
        return id_token.fetch_id_token(Request(), audience)

    def _exchange_token_for_databricks(self, subject_token: str) -> str:
        """Exchange GCP identity token for Databricks access token."""
        response = requests.post(
            self.token_exchange_url,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "subject_token": subject_token,
                "subject_token_type": "urn:ietf:params:oauth:token-type:id_token",
                "scope": "openid offline_access"
            },
            auth=(self.client_id, self.client_secret),
        )
        if response.status_code != 200:
            raise RuntimeError(f"Databricks token exchange failed: {response.text}")
        return response.json()["access_token"]

    def get_sql_connection(self) -> Any:
        """Authenticate and return a Databricks SQL connection."""
        id_token_str = self._get_google_id_token()
        access_token = self._exchange_token_for_databricks(id_token_str)
        return sql.connect(
            server_hostname=self.databricks_host.replace("https://", ""),
            http_path=self.http_path,
            access_token=access_token
        )

    