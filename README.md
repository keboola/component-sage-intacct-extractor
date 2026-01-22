Sage Intacct Extractor
======================

Keboola component for extracting data from Sage Intacct using OAuth 2.0 authentication.

**Table of Contents:**

[TOC]

## Functionality Notes

This extractor allows you to extract data from Sage Intacct objects (such as CUSTOMER, INVOICE, VENDOR, etc.) into Keboola Storage tables. It supports both full and incremental loading, with automatic refresh token management and cursor-based pagination.

## Prerequisites

- Sage Intacct account with API access
- OAuth 2.0 credentials configured in Keboola
- Company ID from your Sage Intacct instance

## Features

| **Feature**             | **Description**                                                    |
|-------------------------|--------------------------------------------------------------------|
| OAuth 2.0               | Secure authentication with automatic refresh token rotation       |
| Row-Based Configuration | Configure multiple endpoints/objects to extract                   |
| Incremental Loading     | Extract only new or modified records using WHENMODIFIED field     |
| Full Load Support       | Option to extract all data on each run                            |
| Dynamic Schema Discovery| Automatically detect available objects and their fields           |
| Auto Primary Key Detection | Automatically identifies primary keys from API metadata        |
| Cursor-Based Pagination | Efficiently handles large datasets with memory-safe pagination   |

## Supported Endpoints

The component supports all Sage Intacct objects available through the API. Common objects include:

- CUSTOMER - Customer records
- INVOICE - Invoice records
- VENDOR - Vendor/supplier records
- BILL - Bill records
- GLACCOUNT - General ledger accounts
- APBILL - Accounts payable bills
- ARINVOICE - Accounts receivable invoices

To see all available endpoints for your instance, use the **list_endpoints** sync action in the component UI.

## Configuration

### Root Configuration

- **Debug Mode** (optional): Enable debug logging for troubleshooting

### Row Configuration

Each configuration row represents one Sage Intacct object to extract:

#### Object/Endpoint
The Sage Intacct object name to extract (e.g., CUSTOMER, INVOICE, VENDOR). Use the UI dropdown to select from available objects.

#### Fields
Specific fields to extract from the object. Leave empty to extract all available fields. Use the UI dropdown to select specific fields.

#### Initial Incremental Value
ISO datetime string (e.g., `2024-01-01T00:00:00Z`) used for the first incremental run when no state exists. Only applicable when using incremental load type.

#### Destination Settings

- **Table Name**: Name of the output table in Keboola Storage (defaults to endpoint name if not specified)
- **Load Type**:
  - `incremental_load` (default): Extract only new/updated records based on incremental field
  - `full_load`: Extract all records on each run
- **Incremental Field**: Field used for incremental filtering (defaults to `WHENMODIFIED`). Only used when Load Type is incremental_load.
- **Primary Key**: Primary key columns for the output table. Auto-detected from API metadata if left empty.

### Example Configuration

```json
{
  "endpoint": "CUSTOMER",
  "columns": ["CUSTOMERID", "NAME", "EMAIL", "STATUS"],
  "initial_since": "2024-01-01T00:00:00Z",
  "destination": {
    "table_name": "customers",
    "load_type": "incremental_load",
    "incremental_field": "WHENMODIFIED",
    "primary_key": ["CUSTOMERID"]
  }
}
```

## Output

The component creates output tables in Keboola Storage based on your configuration:

- One table per configuration row
- Column names match the field names from Sage Intacct
- Primary keys are set based on configuration or auto-detected from API
- Incremental tables maintain state between runs

## Sync Actions

### list_endpoints
Lists all available Sage Intacct objects/endpoints for your instance. Use this to discover what data you can extract.

### list_columns
Lists all available fields for the selected endpoint, including field types and primary key indicators.

### testConnection
Tests the connection to Sage Intacct API to verify authentication is working correctly.

## State Management

The component maintains state between runs:

- **Refresh Token**: Automatically rotates and stores OAuth refresh tokens
- **Last Run Timestamp**: Tracks the last extraction time for incremental loading
- **Auth ID**: Ensures refresh token matches current authorization

## Development

To customize the local data folder path, update the `docker-compose.yml` file:

```yaml
volumes:
  - ./:/code
  - ./data:/data  # Change ./data to your preferred path
```

Clone this repository and run the component:

```bash
git clone https://github.com/keboola/component-sage-intacct-extractor component-sage-intacct-extractor
cd component-sage-intacct-extractor
docker-compose build
docker-compose run --rm dev
```

Run the test suite and lint checks:

```bash
docker-compose run --rm test
```

## Testing Locally

1. Create a `data/config.json` file with your OAuth credentials and endpoint configuration
2. Create an empty `data/in/state.json` file for the first run
3. Run the component: `docker-compose run --rm dev`
4. Check `data/out/tables/` for extracted data
5. Check `data/out/state.json` for updated state

## Integration

For details about deployment and integration with Keboola, refer to the [deployment section of the developer documentation](https://developers.keboola.com/extend/component/deployment/).

## License

MIT License. See [LICENSE](LICENSE.md) for details.
