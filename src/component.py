import logging

from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import BaseType, ColumnDefinition
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

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


STATE_ENDPOINTS = "endpoints"


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.cfg = Configuration(**self.configuration.parameters)
        self.state = self.get_state_file()
        self.client = self._init_client()

    def run(self):
        cfg = self.cfg

        if not cfg.source.endpoint:
            raise UserException("No endpoint configured.")

        logging.info(f'Downloading data for endpoint "{cfg.source.endpoint}"')

        # Get field metadata (names and types)
        all_fields_metadata = self.client.get_object_fields(cfg.source.endpoint)

        # Determine which fields to extract
        fields_to_extract = (
            {
                name: all_fields_metadata.get(name, "string")
                for name in cfg.source.columns
                if name in all_fields_metadata
            }
            if cfg.source.columns
            else all_fields_metadata
        )

        if not fields_to_extract:
            raise UserException(f"No valid fields found for object: {cfg.source.endpoint}")

        table_name = cfg.destination.table_name or f"{cfg.source.endpoint.replace('/', '_')}.csv"

        # Get incremental value from statefile
        incremental_field = None
        incremental_value = None
        last_incremental_value = None

        endpoint_states = self.state.get(STATE_ENDPOINTS, {})
        new_endpoint_states = endpoint_states.copy() if isinstance(endpoint_states, dict) else {}

        if cfg.destination.incremental:
            incremental_field = cfg.source.incremental_field
            if incremental_field and incremental_field not in all_fields_metadata:
                raise UserException(
                    f"Incremental field '{incremental_field}' is not supported for object '{cfg.source.endpoint}'. "
                    f"Supported fields: {', '.join(sorted(all_fields_metadata.keys()))}"
                )
            endpoint_state = new_endpoint_states.get(cfg.source.endpoint, {})
            incremental_value = endpoint_state.get("last_incremental_value") or cfg.source.initial_since

            if incremental_field and incremental_value:
                logging.info(f"Using incremental filtering: {incremental_field} >= {incremental_value}")

        # Build schema
        schema = {
            col_name: ColumnDefinition(
                data_types=BaseType(dtype=convert_to_keboola_type(sage_type)),
                primary_key=col_name in cfg.destination.primary_key,
            )
            for col_name, sage_type in fields_to_extract.items()
        }

        res_table = self.create_out_table_definition(
            table_name,
            schema=schema,
            primary_key=cfg.destination.primary_key,
            incremental=cfg.destination.incremental,
            has_header=True,
        )

        with SageIntacctWriter(res_table.full_path) as writer:
            total_rows = 0
            for batch in self.client.extract_data(
                cfg.source.endpoint,
                list(fields_to_extract.keys()),
                incremental_field,
                incremental_value,
                cfg.batch_size,
                cfg.source.locations or None,
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

            logging.info(f"Extraction complete for {cfg.source.endpoint}. Total rows: {total_rows}")

            if total_rows > 0:
                self.write_manifest(res_table)

        # Update and save state
        if last_incremental_value:
            new_endpoint_states[cfg.source.endpoint] = {"last_incremental_value": last_incremental_value}
        self.state[STATE_ENDPOINTS] = new_endpoint_states
        self.write_state_file(self.state)

    def _init_client(self) -> SageIntacctClient:
        auth = self.cfg.authorization
        client_id = auth.client_id
        client_secret = auth.client_secret
        username = auth.username

        if not client_id or not client_secret:
            raise UserException(
                "Client ID and Client Secret are required. "
                "Please configure '#client_id' and '#client_secret' under the 'authorization' block."
            )
        if not username:
            raise UserException(
                "Username is required. "
                "Please configure 'username' under the 'authorization' block (format: userId@companyId)."
            )

        config = SageIntacctClientConfig(
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            entity=self.cfg.authorization.entity,
        )
        return SageIntacctClient(config)

    @sync_action("list_locations")
    def list_locations(self):
        locations = self.client.list_locations()
        return [SelectElement(value=loc["id"], label=f"{loc['id']} - {loc.get('name', '')}") for loc in locations]

    @sync_action("list_entities")
    def list_entities(self) -> list[SelectElement]:
        entities = self.client.list_entities()
        return [SelectElement(value=e["id"], label=f"{e['id']} - {e.get('name', '')}") for e in entities]

    @sync_action("list_endpoints")
    def list_endpoints(self):
        return [SelectElement(value=obj) for obj in self.client.list_objects()]

    @sync_action("list_columns")
    def list_columns(self):
        endpoint = self.cfg.source.endpoint
        if not endpoint:
            return []

        fields = self.client.get_object_fields(endpoint)
        return [SelectElement(value=field) for field in fields.keys()]


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
