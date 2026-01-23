import base64
import json
import logging
import os
from datetime import datetime, timezone

import backoff
import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from client import SageIntacctClient
from configuration import Configuration
from writer import SageIntacctWriter

URL_SUFFIX = os.environ.get("KBC_STACKID", "connection.keboola.com").replace("connection.", "")

STATE_AUTH_ID = "auth_id"
STATE_REFRESH_TOKEN = "#refresh_token"
STATE_LAST_RUN = "last_run"


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.cfg = Configuration(**self.configuration.parameters)
        self.refresh_token = None
        self.client = self._init_client()

    def run(self):
        logging.info(f'Downloading data for endpoint "{self.cfg.endpoint}".')

        state = self.get_state_file()

        incremental_field = None
        incremental_value = None

        if self.cfg.destination.incremental:
            incremental_field = self.cfg.destination.incremental_field or "WHENMODIFIED"
            incremental_value = state.get(STATE_LAST_RUN) or self.cfg.initial_since or None

            if incremental_field and incremental_value:
                logging.info(f"Using incremental filtering: {incremental_field} >= {incremental_value}")

        table_name = self.cfg.destination.table_name or f"{self.cfg.endpoint}.csv"

        primary_key = self.cfg.destination.primary_key
        if not primary_key:
            logging.info("Primary key not specified in configuration")
            primary_key = []

        res_table = self.create_out_table_definition(
            table_name,
            primary_key=primary_key,
            incremental=self.cfg.destination.incremental,
        )

        writer = SageIntacctWriter(res_table.full_path)

        total_rows = 0
        for batch in self.client.extract_data(
            self.cfg.endpoint, self.cfg.columns, incremental_field, incremental_value
        ):
            total_rows += len(batch)
            writer.writerows(batch)

            if total_rows % 1000 == 0:
                logging.info(f"Downloaded {total_rows} rows so far.")

        logging.info(f"Extraction complete. Total rows downloaded: {total_rows}")

        if total_rows > 0:
            writer.close()
            for column_name in writer.get_result_columns():
                res_table.add_column(column_name)
            self.write_manifest(res_table)

        credentials = self.configuration.oauth_credentials
        self.write_state_file(
            {
                STATE_REFRESH_TOKEN: self.refresh_token,
                STATE_AUTH_ID: credentials["id"],
                STATE_LAST_RUN: datetime.now(timezone.utc).isoformat(),
            }
        )

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def encrypt(self, token: str) -> str:
        """Encrypt a token using Keboola encryption API."""
        url = f"https://encryption.{URL_SUFFIX}.com/encrypt"
        params = {
            "componentId": self.environment_variables.component_id,
            "projectId": self.environment_variables.project_id,
            "configId": self.environment_variables.config_id,
        }
        headers = {"Content-Type": "text/plain"}

        response = requests.post(url, data=token, params=params, headers=headers)
        response.raise_for_status()
        return response.text

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def update_config_state(self, component_id: str, config_id: str, state: dict, branch_id: str = "default"):
        """Update configuration state via Storage API."""
        if not branch_id:
            branch_id = "default"

        url = (
            f"https://connection.{URL_SUFFIX}/v2/storage/branch/{branch_id}"
            f"/components/{component_id}/configs/{config_id}/state"
        )

        parameters = {"state": json.dumps(state)}
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "X-StorageApi-Token": self.environment_variables.token,
        }
        response = requests.put(url, data=parameters, headers=headers)
        response.raise_for_status()

    def _decode_jwt_payload(self, token: str) -> dict:
        """Decode JWT token payload to extract claims."""
        try:
            # JWT has 3 parts: header.payload.signature
            parts = token.split(".")
            if len(parts) != 3:
                raise ValueError("Invalid JWT token format")

            # Decode the payload (second part)
            payload = parts[1]
            # Add padding if needed for base64 decoding
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += "=" * padding

            decoded_bytes = base64.urlsafe_b64decode(payload)
            return json.loads(decoded_bytes)
        except (ValueError, json.JSONDecodeError) as e:
            raise UserException(f"Failed to decode JWT token: {str(e)}")

    def _save_refresh_token(self, new_refresh_token: str):
        """Callback to immediately save the new refresh token via API."""
        # Always store to self variable - will be saved to statefile at end of run
        self.refresh_token = new_refresh_token

        credentials = self.configuration.oauth_credentials
        logging.debug("Saving new refresh token via Storage API")

        try:
            encrypted_token = self.encrypt(new_refresh_token)
        except requests.exceptions.RequestException:
            logging.warning("Encrypt API is unavailable. Token will be saved to statefile at end of run.")
            return

        new_state = {
            "component": {
                STATE_REFRESH_TOKEN: encrypted_token,
                STATE_AUTH_ID: credentials["id"],
            }
        }

        try:
            self.update_config_state(
                component_id=self.environment_variables.component_id,
                config_id=self.environment_variables.config_id,
                state=new_state,
                branch_id=self.environment_variables.branch_id,
            )
            logging.info("Refresh token saved via Storage API")
        except requests.exceptions.RequestException:
            logging.warning("Storage API unavailable. Token will be saved to statefile at end of run.")

    def _init_client(self) -> SageIntacctClient:
        credentials = self.configuration.oauth_credentials

        if not credentials:
            raise UserException("The configuration is not authorized. Please authorize it first.")

        app_key = credentials.appKey
        app_secret = credentials.appSecret

        try:
            oauth_data = json.loads(credentials.data) if isinstance(credentials.data, str) else credentials.data
        except (json.JSONDecodeError, TypeError) as e:
            raise UserException(f"Failed to parse OAuth credentials: {str(e)}")

        state = self.get_state_file()
        refresh_token = state.get(STATE_REFRESH_TOKEN)
        auth_id = state.get(STATE_AUTH_ID)

        if refresh_token and auth_id == credentials["id"]:
            logging.info("Using refresh token from state file")
            access_token = None
        else:
            refresh_token = oauth_data.get("refresh_token")
            access_token = oauth_data.get("access_token")
            logging.info("Using refresh token from OAuth credentials")

        if not refresh_token:
            raise UserException("Refresh token not found in credentials or state file")

        token_payload = self._decode_jwt_payload(access_token)
        company_id = token_payload.get("cnyId", "")

        client = SageIntacctClient(
            app_key, app_secret, company_id, refresh_token, access_token, on_token_refresh=self._save_refresh_token
        )

        # Store refresh token to self - will be saved to statefile at end of run
        self.refresh_token = client.refresh_token

        self.write_state_file(
            {
                STATE_REFRESH_TOKEN: self.refresh_token,
                STATE_AUTH_ID: credentials["id"],
            }
        )

        return client

    @sync_action("list_endpoints")
    def list_endpoints(self):
        return [SelectElement(value=obj) for obj in self.client.list_objects()]

    @sync_action("list_columns")
    def list_columns(self):
        fields = self.client.get_object_fields(self.cfg.endpoint)
        return [SelectElement(value=field) for field in fields]

    @sync_action("list_primary_keys")
    def list_primary_keys(self):
        fields = self.client.get_object_fields(self.cfg.endpoint)
        return [SelectElement(value=field) for field in fields]

    @sync_action("testConnection")
    def test_connection(self):
        self.client.list_objects()


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
