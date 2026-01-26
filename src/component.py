import base64
import json
import logging
import os
from datetime import datetime, timezone

import backoff
import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import ColumnDefinition, BaseType
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from client import SageIntacctClient
from configuration import Configuration
from writer import SageIntacctWriter


def convert_to_keboola_type(sage_type: str) -> str:
    """Convert Sage Intacct type to Keboola base type."""
    type_mapping = {
        "string": "STRING",
        "boolean": "BOOLEAN",
        "integer": "INTEGER",
        "number": "NUMERIC",
        "date": "DATE",
        "datetime": "TIMESTAMP",
    }
    return type_mapping.get(sage_type.lower(), "STRING")


URL_SUFFIX = os.environ.get("KBC_STACKID", "connection.keboola.com").replace("connection.", "")

STATE_AUTH_ID = "auth_id"
STATE_REFRESH_TOKEN = "#refresh_token"
STATE_LAST_RUN = "last_run"


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.cfg = Configuration(**self.configuration.parameters)
        self.state = self.get_state_file()
        self.client = self._init_client()

    def run(self):
        logging.info(f'Downloading data for endpoint "{self.cfg.endpoint}".')
        self._save_refresh_token()

        # Get field metadata (names and types)
        all_fields_metadata = self.client.get_object_fields(self.cfg.endpoint)

        # Determine which fields to extract
        if self.cfg.columns:
            # User specified columns - filter metadata
            fields_to_extract = {
                name: all_fields_metadata.get(name, "string")
                for name in self.cfg.columns
                if name in all_fields_metadata
            }
        else:
            # Use all available fields
            fields_to_extract = all_fields_metadata

        if not fields_to_extract:
            raise UserException(f"No valid fields found for object: {self.cfg.endpoint}")

        incremental_field = None
        incremental_value = None

        if self.cfg.destination.incremental:
            incremental_field = self.cfg.destination.incremental_field
            incremental_value = self.state.get(STATE_LAST_RUN) or self.cfg.initial_since or None

            if incremental_field and incremental_value:
                logging.info(f"Using incremental filtering: {incremental_field} >= {incremental_value}")

        table_name = self.cfg.destination.table_name or f"{self.cfg.endpoint}.csv"

        primary_key = self.cfg.destination.primary_key
        if not primary_key:
            logging.info("Primary key not specified in configuration")
            primary_key = []

        # Build schema with ColumnDefinition objects
        schema = {
            col_name: ColumnDefinition(
                data_types=BaseType(dtype=convert_to_keboola_type(sage_type)),
                primary_key=col_name in primary_key,
            )
            for col_name, sage_type in fields_to_extract.items()
        }

        # Create table definition with schema
        res_table = self.create_out_table_definition(
            table_name,
            schema=schema,
            primary_key=primary_key,
            incremental=self.cfg.destination.incremental,
        )

        writer = SageIntacctWriter(res_table.full_path)

        total_rows = 0
        for batch in self.client.extract_data(
            self.cfg.endpoint, list(fields_to_extract.keys()), incremental_field, incremental_value
        ):
            total_rows += len(batch)
            writer.writerows(batch)

            if total_rows % 1000 == 0:
                logging.info(f"Downloaded {total_rows} rows so far.")

        logging.info(f"Extraction complete. Total rows downloaded: {total_rows}")

        if total_rows > 0:
            writer.close()
            self.write_manifest(res_table)

        self._save_refresh_token()

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def encrypt(self, token: str) -> str:
        """Encrypt a token using Keboola encryption API."""
        url = f"https://encryption.{URL_SUFFIX}/encrypt"
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

    def _save_refresh_token(self):
        """Save the refresh token via Storage API."""

        state_dict = self.state
        state_dict[STATE_REFRESH_TOKEN] = self.client.refresh_token

        # if self.configuration.action in ("run", ""):
        if True:
            state_dict[STATE_LAST_RUN] = datetime.now(timezone.utc).isoformat()
            self.write_state_file(state_dict)

        # Try to save via Storage API
        if self.environment_variables.stack_id:
            logging.info("Saving refresh token via Storage API")
            try:
                encrypted_token = self.encrypt(self.client.refresh_token)
                new_state = {
                    "component": {
                        STATE_REFRESH_TOKEN: encrypted_token,
                        STATE_AUTH_ID: self.configuration.oauth_credentials["id"],
                    }
                }
                self.update_config_state(
                    component_id=self.environment_variables.component_id,
                    config_id=self.environment_variables.config_id,
                    state=new_state,
                    branch_id=self.environment_variables.branch_id,
                )
                logging.info("Refresh token saved via Storage API")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Failed to save token via Storage API: {e}. Will save to state file at end of run.")

    def _init_client(self) -> SageIntacctClient:
        credentials = self.configuration.oauth_credentials

        if not credentials:
            raise UserException("The configuration is not authorized. Please authorize it first.")

        app_key = credentials.appKey
        app_secret = credentials.appSecret

        refresh_token = self.state.get(STATE_REFRESH_TOKEN)
        auth_id = self.state.get(STATE_AUTH_ID)

        # if refresh_token and auth_id == credentials["id"]:
        if refresh_token:
            logging.info("Using refresh token from state file")
            access_token = None
        else:
            refresh_token = credentials.data.get("refresh_token")
            access_token = credentials.data.get("access_token")
            logging.info("Using refresh token from OAuth credentials")

        if not refresh_token:
            raise UserException("Refresh token not found in credentials or state file")

        token_payload = self._decode_jwt_payload(refresh_token)
        company_id = token_payload.get("cnyId", "")

        # Create client (may refresh token during initialization)
        client = SageIntacctClient(app_key, app_secret, company_id, refresh_token, access_token)

        return client

    @sync_action("list_endpoints")
    def list_endpoints(self):
        return [SelectElement(value=obj) for obj in self.client.list_objects()]

    @sync_action("list_columns")
    def list_columns(self):
        fields = self.client.get_object_fields(self.cfg.endpoint)
        return [SelectElement(value=field) for field in fields.keys()]

    @sync_action("list_primary_keys")
    def list_primary_keys(self):
        fields = self.client.get_object_fields(self.cfg.endpoint)
        return [SelectElement(value=field) for field in fields.keys()]

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
