Get detailed warning information for a successful dbt job run.

This tool retrieves and analyzes successful job runs to identify warnings. Warnings are non-fatal issues that don't cause run failures but may indicate potential problems that need attention. This tool returns structured warning information optimized for proactive monitoring and quality assurance.

## Parameters

- run_id (required): The run ID to analyze for warning information

## Returns

Structured warning information with the following format:

- has_warnings: Boolean indicating if any warnings were found
- warning_steps: List of successful steps containing warnings, each with:
  - target: The dbt target environment (e.g., "prod", "dev")
  - step_name: The name of the step that generated warnings
  - finished_at: Timestamp when the step completed
  - results: List of specific warning details, each with:
    - unique_id: Model/test/source unique identifier
    - relation_name: Database relation name or source name
    - message: Warning message describing the issue
    - status: Always "warn" to distinguish from errors
    - compiled_code: Raw compiled SQL code (optional, for test warnings)
- log_warnings: List of warning details extracted directly from logs (no unique_id), each with the same fields as above (status is "warn"; truncated logs not included)
- summary: Aggregate warning counts:
  - total_warnings: Total number of warnings found
  - test_warnings: Number of test warnings (tests with `severity: warn`)
  - freshness_warnings: Number of source freshness warnings
  - log_warnings: Number of warnings extracted from logs (config, deprecation, etc.)

## Example Usage

```json
{
  "run_id": 456
}
```

## Example Response

```json
{
  "has_warnings": true,
  "warning_steps": [
    {
      "target": "prod",
      "step_name": "Invoke dbt with `dbt test`",
      "finished_at": "2025-10-31 10:15:30.123456+00:00",
      "results": [
        {
          "unique_id": "test.analytics.assert_reasonable_revenue",
          "relation_name": "analytics.fact_orders",
          "message": "Got 5 results, configured to warn if !=0",
          "status": "warn",
          "compiled_code": "SELECT * FROM analytics.fact_orders WHERE revenue < 0"
        }
      ]
    },
    {
      "target": "prod",
      "step_name": "Invoke dbt with `dbt source freshness`",
      "finished_at": "2025-10-31 10:10:15.789012+00:00",
      "results": [
        {
          "unique_id": "source.analytics.raw_data.customers",
          "relation_name": "customers",
          "message": "Source freshness warning: 7200s since last load",
          "status": "warn"
        }
      ]
    }
  ],
  "log_warnings": [
    {
      "unique_id": null,
      "relation_name": null,
      "message": "16:38:40  [WARNING]: Configuration paths exist in your dbt_project.yml file which do not apply to any resources.\nThere are 1 unused configuration paths:\n- models.dbt_amplify.prepared.tutoring",
      "status": "warn"
    }
  ],
  "summary": {
    "total_warnings": 3,
    "test_warnings": 1,
    "freshness_warnings": 1,
    "log_warnings": 1
  }
}
```
