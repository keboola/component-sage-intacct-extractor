import logging
import time
from collections.abc import Generator
from dataclasses import dataclass

import requests
from keboola.component.exceptions import UserException


@dataclass
class SageIntacctClientConfig:
    app_key: str
    app_secret: str
    company_id: str
    refresh_token: str
    access_token: str | None = None


class SageIntacctClient:
    def __init__(self, config: SageIntacctClientConfig):
        self.app_key = config.app_key
        self.app_secret = config.app_secret
        self.company_id = config.company_id
        self._refresh_token = config.refresh_token
        self._access_token = config.access_token
        self._session = requests.Session()
        self._base_url = "https://api.intacct.com/ia/api/v1"

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
                logging.info("Refresh token was rotated")

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
        last_response = None

        for attempt in range(max_retries):
            try:
                response = self._session.request(method, url, headers=headers, timeout=60, **kwargs)
                last_response = response

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

            except requests.exceptions.HTTPError as e:
                if attempt == max_retries - 1:
                    # Try to extract detailed error message from response
                    error_details = str(e)
                    if last_response is not None:
                        try:
                            error_body = last_response.json()
                            error_details = f"{str(e)}\n\nAPI Response: {error_body}"
                        except Exception:
                            try:
                                error_details = f"{str(e)}\n\nAPI Response: {last_response.text}"
                            except Exception:
                                pass
                    raise UserException(f"API request failed after {max_retries} attempts: {error_details}")
                time.sleep(2**attempt)

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

    def get_object_fields(self, object_path: str) -> dict[str, str]:
        """Get object fields with their types. Returns dict {field_name: type}."""
        logging.info(f"Fetching field definitions for object: {object_path}")

        params = {"name": object_path, "schema": "true"}
        response = self._make_request("GET", "/services/core/model", params=params)
        data = response.json()

        result = data.get("ia::result", {})
        fields = {}

        for field_name, field_info in result.get("fields", {}).items():
            if not field_name.startswith("ia::"):
                fields[field_name] = field_info.get("type", "string")

        return fields

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
        batch_size: int = 1000,
    ) -> Generator[list[dict], None, None]:
        logging.info(f"Starting data extraction for object: {object_path}")

        # If no fields specified, get all available fields from model API
        if not fields:
            logging.info("No fields specified, fetching all available fields from model API")
            fields = self.get_object_fields(object_path)
            if not fields:
                raise UserException(f"Could not determine fields for object: {object_path}")

        query_payload = {
            "object": object_path,
            "start": 1,
            "size": batch_size,
            "fields": fields,
        }

        if incremental_field and incremental_value:
            logging.info(f"Using incremental filtering: {incremental_field} >= {incremental_value}")
            query_payload["filters"] = [{"$gte": {incremental_field: incremental_value}}]

        total_records = 0
        batch = []

        # Try the first query and handle field errors
        first_attempt = True

        while True:
            response = self._make_request("POST", "/services/core/query", json=query_payload)
            data = response.json()

            # Check if response contains an error
            result = data.get("ia::result", {})
            if isinstance(result, dict) and "ia::error" in result:
                error_info = result["ia::error"]
                error_msg = error_info.get("message", "")

                # If first query fails due to invalid field, try to extract which field and retry
                if first_attempt and "field does not exist" in error_msg.lower():
                    # Try to extract field name from error placeholders or message
                    placeholders = error_info.get("additionalInfo", {}).get("placeholders", {})
                    problem_field = placeholders.get("FIELD")

                    if problem_field and problem_field in query_payload.get("fields", []):
                        logging.warning(f"Field '{problem_field}' does not exist, removing and retrying")
                        query_payload["fields"] = [f for f in query_payload["fields"] if f != problem_field]
                        first_attempt = False
                        continue

                # If we can't handle the error, raise it
                raise UserException(f"API Error: {error_msg}")

            first_attempt = False

            results = data.get("ia::result", [])
            if not results:
                break

            for record in results:
                # Only include fields that were requested and don't start with "ia::"
                clean_record = {
                    k: v for k, v in record.items() if not k.startswith("ia::") and k in query_payload.get("fields", [])
                }
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
