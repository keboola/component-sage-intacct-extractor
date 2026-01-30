Sage Intacct Extractor
======================

Keboola component for extracting data from Sage Intacct using OAuth 2.0 authentication.

**Table of Contents:**
<!-- vscode-markdown-toc -->
* 1. [Functionality Notes](#FunctionalityNotes)
* 2. [Prerequisites](#Prerequisites)
* 3. [Features](#Features)
* 4. [Supported Endpoints](#SupportedEndpoints)
* 5. [Configuration](#Configuration)
	* 5.1. [Endpoints](#Endpoints)
	* 5.2. [Destination](#Destination)
	* 5.3. [Additional Settings](#AdditionalSettings)
	* 5.4. [Example Configuration](#ExampleConfiguration)
* 6. [Output](#Output)
* 7. [Sync Actions](#SyncActions)
	* 7.1. [list_endpoints](#list_endpoints)
	* 7.2. [list_columns](#list_columns)
* 8. [State Management](#StateManagement)
* 9. [Development](#Development)
* 10. [Testing Locally](#TestingLocally)
* 11. [Integration](#Integration)
* 12. [License](#License)

<!-- vscode-markdown-toc-config
	numbering=true
	autoSave=true
	/vscode-markdown-toc-config -->
<!-- /vscode-markdown-toc -->


##  1. <a name='FunctionalityNotes'></a>Functionality Notes

This extractor allows you to extract data from Sage Intacct objects (such as CUSTOMER, INVOICE, VENDOR, etc.) into Keboola Storage tables. It supports both full and incremental loading, with automatic refresh token management and cursor-based pagination.

##  2. <a name='Prerequisites'></a>Prerequisites

- Sage Intacct account with API access
- OAuth 2.0 credentials configured in Keboola
- Company ID from your Sage Intacct instance

##  3. <a name='Features'></a>Features

| **Feature**             | **Description**                                                    |
|-------------------------|--------------------------------------------------------------------|
| OAuth 2.0               | Secure authentication with automatic refresh token rotation       |
| Multiple Endpoints      | Configure multiple endpoints/objects to extract in a single run   |
| Incremental Loading     | Extract only new or modified records using configurable field     |
| Full Load Support       | Option to extract all data on each run                            |
| Dynamic Schema Discovery| Automatically detect available objects and their fields           |
| Configurable Primary Keys | Specify primary key columns for each endpoint                   |
| Cursor-Based Pagination | Efficiently handles large datasets with memory-safe pagination   |

##  4. <a name='SupportedEndpoints'></a>Supported Endpoints

The component supports all Sage Intacct objects available through the API. Common objects include:

- CUSTOMER - Customer records
- INVOICE - Invoice records
- VENDOR - Vendor/supplier records
- BILL - Bill records
- GLACCOUNT - General ledger accounts
- APBILL - Accounts payable bills
- ARINVOICE - Accounts receivable invoices

To see all available endpoints for your instance, use the **list_endpoints** sync action in the component UI.

##  5. <a name='Configuration'></a>Configuration

###  5.1. <a name='Endpoints'></a>Endpoints

Configure one or more Sage Intacct objects to extract. Each endpoint configuration includes:

- **Object/Endpoint**: The Sage Intacct object path to extract (e.g., `accounts-receivable/customer`, `general-ledger/account`). Use the UI dropdown to select from available objects.
- **Fields**: Specific fields to extract from the object. Leave empty to extract all available fields. Use the UI dropdown to select specific fields.
- **Table Name** (optional): Name of the output table in Keboola Storage (defaults to endpoint name if not specified)
- **Primary Key**: Primary key columns for the output table (defaults to `["id"]`)
- **Incremental Field**: Field used for incremental filtering (defaults to `id`). Only used when Load Type is set to incremental load.
- **Initial Incremental Value** (optional): ISO datetime string (e.g., `2024-01-01T00:00:00Z`) used for the first incremental run when no state exists. Only applicable when using incremental load type.

###  5.2. <a name='Destination'></a>Destination

Global destination settings that apply to all endpoints:

- **Load Type**:
  - `incremental_load` (default): Extract only new/updated records based on incremental field
  - `full_load`: Extract all records on each run

###  5.3. <a name='AdditionalSettings'></a>Additional Settings

- **Batch Size** (optional): Number of records to fetch per API request (default: 1000, min: 1, max: 10000)
- **Debug Mode** (optional): Enable debug logging for troubleshooting

###  5.4. <a name='ExampleConfiguration'></a>Example Configuration

```json
{
  "endpoints": [
    {
      "endpoint": "accounts-receivable/customer",
      "columns": [],
      "table_name": "customers",
      "primary_key": ["id"],
      "incremental_field": "id",
      "initial_since": "2024-01-01T00:00:00Z"
    }
  ],
  "destination": {
    "load_type": "incremental_load"
  },
  "batch_size": 1000,
  "debug": false
}
```

##  6. <a name='Output'></a>Output

The component creates output tables in Keboola Storage based on your configuration:

- One table per configured endpoint
- Column names match the field names from Sage Intacct
- Primary keys are set based on configuration (defaults to `["id"]`)
- Incremental tables maintain state between runs for each endpoint

##  7. <a name='SyncActions'></a>Sync Actions

###  7.1. <a name='list_endpoints'></a>list_endpoints
Lists all available Sage Intacct objects/endpoints for your instance. Use this to discover what data you can extract.

###  7.2. <a name='list_columns'></a>list_columns
Lists all available fields for the selected endpoint, including field types.

##  8. <a name='StateManagement'></a>State Management

The component maintains state between runs:

- **Refresh Token**: Automatically rotates and stores OAuth refresh tokens
- **Last Incremental Value**: Tracks the last incremental value per endpoint for incremental loading
- **Auth ID**: Ensures refresh token matches current authorization

##  9. <a name='Development'></a>Development

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

##  10. <a name='TestingLocally'></a>Testing Locally

1. Create a `data/config.json` file with your OAuth credentials and endpoint configuration
2. Create an empty `data/in/state.json` file for the first run
3. Run the component: `docker-compose run --rm dev`
4. Check `data/out/tables/` for extracted data
5. Check `data/out/state.json` for updated state

##  11. <a name='Integration'></a>Integration

For details about deployment and integration with Keboola, refer to the [deployment section of the developer documentation](https://developers.keboola.com/extend/component/deployment/).

##  12. <a name='License'></a>License

MIT License. See [LICENSE](LICENSE.md) for details.
