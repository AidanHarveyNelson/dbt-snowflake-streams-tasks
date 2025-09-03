{%- macro create_stream(source_table) -%}
    {%- do log('Source Table is: ' ~ source_table, info = true) -%}
    {%- set name = config.require('stream').name -%}
    {%- do log('Creating Stream: ' ~ name, info = true) -%}
    {%- set args_sql -%}
        APPEND_ONLY = {{ config.get('stream').append_only|upper or 'TRUE' }}
        COMMENT = '{{ config.get('stream').description or '' }}'
        {{- "COPY GRANTS" ~ config.get('stream').copy_grants if config.get('stream').copy_grants else '' -}}
    {%- endset -%}
    {%- set run_sql -%}
        CREATE OR REPLACE STREAM {{ name }} ON TABLE {{ source_table }}
        {{ args_sql }}
    {%- endset -%}
    {{ run_sql }}
{%- endmacro -%}
