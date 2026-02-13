import base64
import json
import logging
import os
from io import StringIO

import backoff
import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import BaseType, ColumnDefinition
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement
from wurlitzer import pipes

from client import SageIntacctClient, SageIntacctClientConfig
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
STATE_ENDPOINTS = "endpoints"


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.cfg = Configuration(**self.configuration.parameters)
        self.state = self.get_state_file()
        self.client = self._init_client()

    def run(self):
        self._save_refresh_token()

        # Start with existing endpoint states from statefile
        endpoint_states = self.state.get(STATE_ENDPOINTS, {})
        # Ensure it's a dict (handle old state format if it's a list)
        new_endpoint_states = endpoint_states.copy() if isinstance(endpoint_states, dict) else {}

        for endpoint_config in self.cfg.endpoints:
            logging.info(f'Downloading data for endpoint "{endpoint_config.endpoint}"')

            # Get field metadata (names and types)
            all_fields_metadata = self.client.get_object_fields(endpoint_config.endpoint)

            # Determine which fields to extract
            fields_to_extract = (
                {
                    name: all_fields_metadata.get(name, "string")
                    for name in endpoint_config.columns
                    if name in all_fields_metadata
                }
                if endpoint_config.columns
                else all_fields_metadata
            )

            if not fields_to_extract:
                raise UserException(f"No valid fields found for object: {endpoint_config.endpoint}")

            table_name = endpoint_config.table_name or f"{endpoint_config.endpoint.replace('/', '_')}.csv"

            # Get incremental value from statefile for this endpoint
            incremental_field = None
            incremental_value = None
            last_incremental_value = None

            if self.cfg.destination.incremental:
                incremental_field = endpoint_config.incremental_field
                endpoint_state = new_endpoint_states.get(endpoint_config.endpoint, {})
                incremental_value = endpoint_state.get("last_incremental_value") or endpoint_config.initial_since

                if incremental_field and incremental_value:
                    logging.info(f"Using incremental filtering: {incremental_field} >= {incremental_value}")

            # Build schema
            schema = {
                col_name: ColumnDefinition(
                    data_types=BaseType(dtype=convert_to_keboola_type(sage_type)),
                    primary_key=col_name in endpoint_config.primary_key,
                )
                for col_name, sage_type in fields_to_extract.items()
            }

            res_table = self.create_out_table_definition(
                table_name,
                schema=schema,
                primary_key=endpoint_config.primary_key,
                incremental=self.cfg.destination.incremental,
            )

            with SageIntacctWriter(res_table.full_path) as writer:
                total_rows = 0
                for batch in self.client.extract_data(
                    endpoint_config.endpoint,
                    list(fields_to_extract.keys()),
                    incremental_field,
                    incremental_value,
                    self.cfg.batch_size,
                    self.cfg.locations or None,
                ):
                    total_rows += len(batch)
                    writer.writerows(batch)

                    # Track the last incremental value from this batch
                    if incremental_field and batch:
                        for row in batch:
                            if incremental_field in row and row[incremental_field]:
                                last_incremental_value = row[incremental_field]

                    if total_rows % 1000 == 0:
                        logging.info(f"Downloaded {total_rows} rows so far")

                logging.info(f"Extraction complete for {endpoint_config.endpoint}. Total rows: {total_rows}")

                if total_rows > 0:
                    self.write_manifest(res_table)

            # Update state for this endpoint
            if last_incremental_value:
                new_endpoint_states[endpoint_config.endpoint] = {"last_incremental_value": last_incremental_value}

        # Save state (preserves endpoints not in current config)
        self.state[STATE_ENDPOINTS] = new_endpoint_states
        self.write_state_file(self.state)
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
        """Save the refresh token and state via Storage API."""

        state_dict = self.state
        state_dict[STATE_REFRESH_TOKEN] = self.client.refresh_token
        state_dict[STATE_AUTH_ID] = self.configuration.oauth_credentials["id"]
        self.write_state_file(state_dict)

        # Try to save via Storage API
        if self.environment_variables.stack_id:
            logging.info("Saving state via Storage API")
            try:
                encrypted_token = self.encrypt(self.client.refresh_token)
                new_state = {
                    "component": {
                        STATE_REFRESH_TOKEN: encrypted_token,
                        STATE_AUTH_ID: self.configuration.oauth_credentials["id"],
                        STATE_ENDPOINTS: state_dict.get(STATE_ENDPOINTS, {}),
                    }
                }
                self.update_config_state(
                    component_id=self.environment_variables.component_id,
                    config_id=self.environment_variables.config_id,
                    state=new_state,
                    branch_id=self.environment_variables.branch_id,
                )
                logging.info("State saved via Storage API")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Failed to save state via Storage API: {e}. Will save to state file at end of run.")

    def _init_client(self) -> SageIntacctClient:
        credentials = self.configuration.oauth_credentials

        if not credentials:
            raise UserException("The configuration is not authorized. Please authorize it first.")

        app_key = credentials.appKey
        app_secret = credentials.appSecret

        refresh_token = self.state.get(STATE_REFRESH_TOKEN)
        auth_id = self.state.get(STATE_AUTH_ID)

        if refresh_token and auth_id == credentials["id"]:
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

        # Create client config
        config = SageIntacctClientConfig(
            app_key=app_key,
            app_secret=app_secret,
            company_id=company_id,
            refresh_token=refresh_token,
            access_token=access_token,
        )

        # Create client (may refresh token during initialization)
        client = SageIntacctClient(config)

        return client

    @sync_action("list_endpoints")
    def list_endpoints(self):
        out = StringIO()
        with pipes(stdout=out, stderr=out):
            result = [SelectElement(value=obj) for obj in self.client.list_objects()]
            self._save_refresh_token()
            return result

    @sync_action("list_columns")
    def list_columns(self):
        out = StringIO()
        with pipes(stdout=out, stderr=out):
            # When called from within an endpoint array item, get the endpoint value from parameters
            endpoint = self.configuration.parameters.get("endpoint")
            if not endpoint:
                return []

            fields = self.client.get_object_fields(endpoint)
            self._save_refresh_token()
            return [SelectElement(value=field) for field in fields.keys()]

    @sync_action("list_locations")
    def list_locations(self):
        out = StringIO()
        with pipes(stdout=out, stderr=out):
            locations = self.client.list_locations()
            self._save_refresh_token()
            return [SelectElement(value=loc["id"], label=loc["name"]) for loc in locations]


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
