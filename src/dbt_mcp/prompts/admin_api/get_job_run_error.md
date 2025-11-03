Get focused error information for a failed dbt job run.

This tool retrieves and analyzes job run failures to provide concise, actionable error details optimized for troubleshooting. Instead of verbose run details, it returns structured error information with minimal token usage.

## Parameters

- run_id (required): The run ID to analyze for error information

## Returns

Structured error information with `failed_steps` containing a list of failed step details:

- failed_steps: List of failed steps, each containing:
  - target: The dbt target environment where the failure occurred
  - step_name: The failed step that caused the run to fail
  - finished_at: Timestamp when the failed step completed
  - results: List of specific error details, each with:
    - unique_id: Model/test unique identifier (nullable)
    - relation_name: Database relation name or "No database relation"
    - message: Error message
    - compiled_code: Raw compiled SQL code (nullable)
    - truncated_logs: Raw truncated debug log output (nullable)

NOTE: The "truncated_logs" key only populates if there is no `run_results.json` artifact to parse after a job run error.

## Example Usage

```json
{
  "run_id": 789
}
```

## Example Response

```json
{
  "failed_steps": [
    {
      "target": "prod",
      "step_name": "Invoke dbt with `dbt run --models staging`",
      "finished_at": "2025-09-17 14:32:15.123456+00:00",
      "results": [
        {
          "unique_id": "model.analytics.stg_users",
          "relation_name": "analytics_staging.stg_users",
          "message": "Syntax error: Expected end of input but got keyword SELECT at line 15",
          "compiled_code": "SELECT\n  id,\n  name\nFROM raw_users\nSELECT -- duplicate SELECT causes error",
          "truncated_logs": null
        }
      ]
    }
  ]
}
```