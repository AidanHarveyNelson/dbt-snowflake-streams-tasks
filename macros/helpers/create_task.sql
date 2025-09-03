{%- macro create_task(stream, target_table, sql, tmp_relation, unique_key) -%}
    {%- do log('Target Table is: ' ~ target_table, info = true) -%}
    {%- set name = config.require('task').name -%}
    {%- do log('Creating Task: ' ~ name, info = true) -%}
    {# This SQL has formatting issues think of a better way to populated and validate the config#}
    {%- set args_sql -%}
        COMMENT = '{{ config.get('stream').description or '' }}'
        {{ "WAREHOUSE = " ~ config.get('task').warehouse|upper if config.get('task').warehouse|upper else '' }}
        {{ "SCHEDULE = '" ~ config.get('task').schedule|upper ~ "'" if config.get('task').schedule|upper else '' -}}
        {{ "EXECUTE AS USER = '" ~ config.get('task').execute_as_user|upper ~ "'" if config.get('task').execute_as_user|upper else '' -}}
        {{ "ALLOW_OVERLAPPING_EXECUTION = " ~ config.get('task').allow_overlapping_execution|upper if config.get('task').allow_overlapping_execution else '' -}}
        {{ "USER_TASK_TIMEOUT_MS = " ~ config.get('task').user_task_timeout_ms if config.get('task').user_task_timeout_ms else '' -}}
        {{ "SUSPEND_TASK_AFTER_NUM_FAILURES = " ~ config.get('task').suspend_task_after_num_failures if config.get('task').suspend_task_after_num_failures else '' -}}
        {{ "ERROR_INTEGRATION = '" ~ config.get('task').error_integration|upper ~ "'" if config.get('task').error_integration|upper else '' -}}
        {{ "SUCCESS_INTEGRATION = '" ~ config.get('task').success_integration|upper ~ "'" if config.get('task').success_integration|upper else '' -}}
        {{ "LOG_LEVEL = '" ~ config.get('task').log_level|upper ~ "'" if config.get('task').log_level|upper else '' -}}
        {{ "FINALIZE = '" ~ config.get('task').finalize|upper ~ "'" if config.get('task').finalize|upper else '' -}}
        {{ "TASK_AUTO_RETRY_ATTEMPTS = " ~ config.get('task').task_auto_retry_attempts if config.get('task').task_auto_retry_attempts else '' -}}
        {{ "USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = " ~ config.get('task').user_task_minimum_trigger_interval_in_seconds if config.get('task').user_task_minimum_trigger_interval_in_seconds else '' -}}
        {{ "TARGET_COMPLETION_INTERVAL = '" ~ config.get('task').target_completion_interval|upper ~ "'" if config.get('task').target_completion_interval|upper else '' -}}
        {{ "SERVERLESS_TASK_MIN_STATEMENT_SIZE = '" ~ config.get('task').serverless_task_min_statement_size|upper ~ "'" if config.get('task').serverless_task_min_statement_size|upper else '' -}}
        {{ "SERVERLESS_TASK_MAX_STATEMENT_SIZE = '" ~ config.get('task').serverless_task_max_statement_size|upper ~ "'" if config.get('task').serverless_task_max_statement_size|upper else '' }}
        {{ "WHEN \n            " ~ config.get('task').when|upper if config.get('task').when|upper else '' }}
    {%- endset -%}
    {%- do log('Printing Args SQL: ' ~ args_sql, info = true) -%}
    {%- set columns = adapter.get_columns_in_relation(tmp_relation) | rejectattr('name', 'equalto', 'METADATA$ACTION')
                | rejectattr('name', 'equalto', 'METADATA$ISUPDATE')
                | rejectattr('name', 'equalto', 'METADATA$ROW_ID')
                | list -%}
    {%- set merge_task -%}
        MERGE INTO  {{target_table }} TARGET
        USING {{ stream }} SOURCE
        ON ({%- for key in unique_key -%}
        TARGET.{{ key }} = SOURCE.{{ key }}{%- if not loop.last -%} AND {%- endif -%}{%- endfor -%}
        )
        -- Mode DELETE
        WHEN MATCHED AND SOURCE.METADATA$ACTION = 'DELETE' AND SOURCE.METADATA$ISUPDATE = 'FALSE' THEN
            DELETE
        -- Mode UPDATE
        WHEN MATCHED AND SOURCE.METADATA$ACTION = 'INSERT' THEN 
        UPDATE SET {% for col in columns %}
            TARGET.{{ col.name }} = SOURCE.{{ col.name }}{%- if not loop.last -%},{%- endif -%}{%- endfor -%}
        -- Mode INSERT
        WHEN NOT MATCHED AND SOURCE.METADATA$ACTION = 'INSERT' THEN INSERT
            ( {%- for col in columns -%}
            {{- col.name -}}{%- if not loop.last -%},{%- endif -%}{%- endfor -%}
            )
        VALUES
            ({%- for col in columns -%}
            SOURCE.{{- col.name -}}{%- if not loop.last -%},{%- endif -%}{%- endfor -%}
            )
    {%- endset -%}
    {%- set run_sql -%}
        CREATE OR ALTER TASK {{ name }}
        {{ args_sql }}
        AS
        {{ merge_task }}
    {%- endset -%}
    {{ run_sql }}
{%- endmacro -%}
