import logging
import time
from collections.abc import Callable, Generator

import requests
from keboola.component.exceptions import UserException


class SageIntacctClient:
    def __init__(
        self,
        app_key: str,
        app_secret: str,
        company_id: str,
        refresh_token: str,
        access_token: str | None = None,
        on_token_refresh: Callable[[str], None] | None = None,
    ):
        self.app_key = app_key
        self.app_secret = app_secret
        self.company_id = company_id
        self._refresh_token = refresh_token
        self._access_token = access_token
        self._session = requests.Session()
        self._base_url = "https://api.intacct.com/ia/api/v1"
        self._on_token_refresh = on_token_refresh

        if not self._access_token:
            self._authenticate()

    def _authenticate(self):
        logging.info("Authenticating with Sage Intacct API")
        token_url = "https://api.intacct.com/ia/api/v1/oauth2/token"

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
            "client_id": self.app_key,
            "client_secret": self.app_secret,
        }

        try:
            response = self._session.post(token_url, data=payload, timeout=30)
            response.raise_for_status()

            token_data = response.json()
            self._access_token = token_data.get("access_token")
            if "refresh_token" in token_data:
                self._refresh_token = token_data["refresh_token"]
                logging.info("Refresh token was rotated, updating stored token")
                # Notify component to save the new refresh token immediately
                if self._on_token_refresh:
                    self._on_token_refresh(self._refresh_token)

            logging.info("Successfully authenticated with Sage Intacct")

        except requests.exceptions.RequestException as e:
            raise UserException(f"Authentication failed: {str(e)}")

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        if not self._access_token:
            self._authenticate()

        url = f"{self._base_url}{endpoint}"
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self._access_token}"

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self._session.request(method, url, headers=headers, timeout=60, **kwargs)

                if response.status_code == 401:
                    logging.info("Access token expired, refreshing...")
                    self._authenticate()
                    headers["Authorization"] = f"Bearer {self._access_token}"
                    continue

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logging.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                return response

            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    raise UserException(f"API request failed after {max_retries} attempts: {str(e)}")
                time.sleep(2**attempt)

        raise UserException("API request failed: max retries exceeded")

    def list_objects(self) -> list[str]:
        logging.info("Fetching list of Sage Intacct objects from model API")

        response = self._make_request("GET", "/services/core/model")
        results = response.json().get("ia::result", [])

        objects = [
            item["apiObject"]
            for item in results
            if item.get("apiObject")
            and item.get("type", "") in ["rootObject", "ownedObject", "object"]
            and "GET" in item.get("httpMethods", "")
        ]

        logging.info(f"Found {len(objects)} objects from model API")
        return objects

    def get_object_fields(self, object_path: str) -> list[str]:
        logging.info(f"Fetching field definitions for object: {object_path}")

        params = {"name": object_path, "schema": "true"}
        response = self._make_request("GET", "/services/core/model", params=params)
        data = response.json()

        result = data.get("ia::result")

        if not result:
            logging.warning(f"No model information found for {object_path}")
            return []

        if isinstance(result, list):
            if not result:
                return []
            result = result[0]

        fields = []
        model_fields = result.get("fields", {})
        model_groups = result.get("groups", {})

        if not model_fields:
            logging.warning(f"No fields found in model for {object_path}")
            return self._get_fields_from_data(object_path)

        for field_name in model_fields.keys():
            if not field_name.startswith("ia::"):
                fields.append(field_name)

        for group_info in model_groups.values():
            group_fields = group_info.get("fields", {})
            for field_name in group_fields.keys():
                if not field_name.startswith("ia::"):
                    fields.append(field_name)

        logging.info(f"Found {len(fields)} fields for {object_path}")
        return fields

    def get_primary_key(self, object_path: str) -> list[str]:
        return []

    def _get_fields_from_data(self, object_path: str) -> list[str]:
        logging.info(f"Falling back to data inference for fields: {object_path}")

        query_payload = {
            "object": object_path,
            "start": 1,
            "size": 1,
        }

        response = self._make_request("POST", "/services/core/query", json=query_payload)
        data = response.json()

        results = data.get("ia::result", [])

        if not results:
            logging.warning(f"No records found for {object_path}, cannot determine fields")
            return []

        fields = []
        first_record = results[0]

        for field_name in first_record.keys():
            if not field_name.startswith("ia::"):
                fields.append(field_name)

        logging.info(f"Inferred {len(fields)} fields from data for {object_path}")
        return fields

    def extract_data(
        self,
        object_path: str,
        fields: list[str],
        incremental_field: str | None = None,
        incremental_value: str | None = None,
    ) -> Generator[list[dict], None, None]:
        logging.info(f"Starting data extraction for object: {object_path}")

        query_payload = {
            "object": object_path,
            "start": 1,
            "size": 1000,
        }

        if fields:
            query_payload["fields"] = fields

        if incremental_field and incremental_value:
            logging.info(f"Using incremental filtering: {incremental_field} >= {incremental_value}")
            query_payload["filters"] = [{"$gte": {incremental_field: incremental_value}}]
            query_payload["filterExpression"] = "1"

        total_records = 0
        batch = []

        while True:
            response = self._make_request("POST", "/services/core/query", json=query_payload)
            data = response.json()

            results = data.get("ia::result", [])
            if not results:
                break

            for record in results:
                clean_record = {k: v for k, v in record.items() if not k.startswith("ia::")}
                batch.append(clean_record)
                total_records += 1

                if len(batch) >= 100:
                    logging.info(f"Extracted {total_records} records so far...")
                    yield batch
                    batch = []

            next_start = data.get("ia::meta", {}).get("next")
            if not next_start:
                break

            query_payload["start"] = next_start
            logging.debug(f"Continuing pagination from record {next_start}...")

        if batch:
            yield batch

        logging.info(f"Extraction complete. Total records: {total_records}")

    @property
    def refresh_token(self) -> str:
        return self._refresh_token
