Get the name, description, and metadata of all dbt sources in the environment. Sources represent external data tables that your dbt models build upon.

Parameters (all optional):
- source_names: List of specific source names to filter by (e.g., ['raw_data', 'external_api'])
- unique_ids: List of specific source table IDs to filter by

Returns information including:
- name: The table name within the source
- uniqueId: The unique identifier for this source table
- identifier: The underlying table identifier in the warehouse
- description: Description of the source table
- sourceName: The source name (e.g., 'raw_data', 'external_api')
- database: Database containing the source table
- schema: Schema containing the source table
- resourceType: Will be 'source'
- freshness: Real-time freshness status from production including:
  - maxLoadedAt: When the source was last loaded
  - maxLoadedAtTimeAgoInS: How long ago the source was loaded (in seconds)
  - freshnessStatus: Current freshness status (e.g., 'pass', 'warn', 'error')

This tool is useful for:
- Data discovery and understanding available source tables
- Lineage analysis to see what external data feeds into your dbt project
- Source validation and freshness monitoring
- Filtering sources by environment, status, or organizational tags
- Getting a complete picture of your data graph including upstream dependencies

Notes:
- Filtering by `source_names` returns every table under each matching source definition, mirroring Discovery API behaviour.
