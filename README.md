Sage Intacct Extractor
======================

Extracts data from Sage Intacct objects into Keboola Storage via the REST API.

**Table of Contents:**

<!-- TOC -->
* [Sage Intacct Extractor](#sage-intacct-extractor)
* [Functionality Notes](#functionality-notes)
* [Prerequisites](#prerequisites)
* [Features](#features)
* [Supported Endpoints](#supported-endpoints)
* [Configuration](#configuration)
  * [Authorization](#authorization)
  * [Row Configuration](#row-configuration)
* [State Management](#state-management)
* [Development](#development)
* [Integration](#integration)
<!-- TOC -->

Functionality Notes
===================

The component reads from a Sage Intacct object and writes records to a Keboola output table. It supports both full and incremental loading with cursor-based pagination. Each configuration row extracts one endpoint.

**API Rate Limits:** Sage Intacct Tier 1 includes 100,000 API requests per month for free. Use incremental load to minimize API usage.

Prerequisites
=============

- A Sage Intacct account with a Web Services license.
- A registered OAuth application — [Sage documentation](https://developer.sage.com/intacct/docs/1/sage-intacct-rest-api/get-started/quick-start)
- The application authorized in your Intacct instance — [Documentation](https://www.intacct.com/ia/docs/en_US/help_action/Company/Company_setup/Company_Information/Security/company-authorized-client-applications.htm)
- A Web Services user created in your Intacct instance — [Documentation](https://www.intacct.com/ia/docs/en_US/help_action/Administration/Users/web-services-only-users.htm)

Features
========

| **Feature**              | **Description**                                                   |
|--------------------------|-------------------------------------------------------------------|
| Generic UI Form          | Dynamic UI form with endpoint and field auto-loading.             |
| Client Credentials Auth  | OAuth2 client credentials flow with username.                     |
| Incremental Loading      | Extract only new or modified records using a configurable field.  |
| Full Load Support        | Option to extract all records on each run.                        |
| Dynamic Schema Discovery | Automatically detect available objects and their fields.          |
| Location Filtering       | Filter extracted data by Sage Intacct location.                   |
| Cursor-Based Pagination  | Efficiently handles large datasets with memory-safe pagination.   |

Supported Endpoints
===================

All readable Sage Intacct objects are supported. The list of available endpoints is loaded dynamically from the Sage Intacct model API. Use the **Re-load endpoints** dropdown to browse available objects for your instance.

Configuration
=============

Authorization
-------------

The root configuration holds credentials shared across all rows.

- **Client ID** — Your Sage Intacct application Client ID (stored encrypted).
- **Client Secret** — Your Sage Intacct application Client Secret (stored encrypted).
- **Username** — The user to authenticate as, in the format `userId@companyId`.
- **Entity** — (Optional) The entity context for all requests. Use the **Load entities** button to browse available options.

Row Configuration
-----------------

Each configuration row defines one extraction from one Sage Intacct object.

**Source**

- **Locations** — (Optional) Filter by location. Leave empty to extract all locations. Use **Re-load locations** to browse.
- **Endpoint** — The Sage Intacct object to extract (e.g., `accounts-receivable/customer`). Use **Re-load endpoints** to browse.
- **Fields** — (Optional) Columns to extract. Leave empty to extract all available fields. Use **Re-load columns** to browse.
- **Incremental Field** — Field used to filter new/updated records (default: `id`). Only applies to incremental load.
- **Initial Incremental Value** — (Optional) ISO datetime for the first incremental run when no state exists (e.g., `2024-01-01T00:00:00Z`).

**Destination**

- **Load Type** — `incremental_load` upserts new/updated records; `full_load` overwrites the table on every run.
- **Table Name** — (Optional) Output table name in Keboola Storage. Defaults to the endpoint name.
- **Primary Key** — Columns that uniquely identify each record (default: `id`).

**Additional Settings**

- **Batch Size** — Records per API request (default: 1000, range: 100–4000).
- **Debug Mode** — Enables verbose logging for troubleshooting.

State Management
================

The component maintains state between runs per config row. The last extracted incremental value is stored and used to filter records on subsequent incremental runs.

Development
===========

To customize the local data folder path, replace `CUSTOM_FOLDER` with your desired path in `docker-compose.yml`:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository and run the component:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/keboola/component-sage-intacct-extractor component-sage-intacct-extractor
cd component-sage-intacct-extractor
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint checks:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For details about deployment and integration with Keboola, refer to the [deployment section of the developer documentation](https://developers.keboola.com/extend/component/deployment/).
