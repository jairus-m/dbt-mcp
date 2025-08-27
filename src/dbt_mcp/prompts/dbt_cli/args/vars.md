Variables can be passed to dbt commands using the vars parameter. Variables can be accessed in dbt code using `{{ var('variable_name') }}`.

Supported formats:
- Single variable (curly brackets optional): `variable_name: value`
- Multiple variables (curly brackets needed): `{"key1": "value1", "key2": "value2"}`
- Mixed types: `{"string_var": "hello", "number_var": 42, "boolean_var": true}`