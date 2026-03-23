## Authorization

Enter your Sage Intacct OAuth 2.0 credentials. These are shared across all configuration rows.

- **Client ID / Client Secret** — from your Sage Intacct developer application
- **Username** — in the format `userId@companyId`
- **Entity** (optional) — scope requests to a specific entity; use *Load entities* to browse available options

## Source (per row)

- **Locations** (optional) — filter by location; leave empty to extract all
- **Endpoint** — Sage Intacct object to extract (e.g. `accounts-receivable/customer`); use *Load endpoints* to browse
- **Fields** (optional) — columns to extract; leave empty for all fields
- **Incremental Field** — field used to filter new/updated records (default: `id`); only applies to incremental load
- **Initial Incremental Value** (optional) — ISO datetime used on the first run when no state exists (e.g. `2024-01-01T00:00:00Z`)

## Destination (per row)

- **Load Type** — `incremental_load` upserts new/updated records; `full_load` overwrites the table on every run
- **Table Name** (optional) — output table name; defaults to the endpoint name
- **Primary Key** — columns that uniquely identify each record (default: `id`)

## Additional Settings

- **Batch Size** — records per API request (default: 1000, range: 100–4000)
- **Debug Mode** — enables verbose logging for troubleshooting
