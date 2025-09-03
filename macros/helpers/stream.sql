{%- macro stream_source(source_name, table_name) -%}
    {{ dbt_snowflake_streams_tasks.stream('source', source_name, table_name) }}
{%- endmacro -%}

{%- macro stream_ref(model_name) -%}
    {{ dbt_snowflake_streams_tasks.stream('model', none, model_name) }}
{%- endmacro -%}


{%- macro stream(model_type, source_name, model_name) -%}

    {%- set full_refresh = flags.FULL_REFRESH == TRUE -%}
    {# Need to convert this to process a list of passed in streams #}

    {%- set stream_name = '' -%}
    {%- set target_model = '' -%}

    {%- if not execute -%}
        {% do log('Running During Parsing Mode', info=true)%}
        {% do log('Setting up Stream Configuration For Model Type: ' ~ model_type ~ ' Source: ' ~ source_name ~ ' Model Name: ' ~ model_name, info = true) %}

        {% do config.set('src_table', {
            'model_type': model_type,
            'source_name': source_name,
            'model_name': model_name
        }) %}
    {%- endif -%}

    {%- if model_type == 'model' -%}
        {%- set target_model = ref(model_name) -%}
    {% elif model_type == 'source' %}
        {%- set target_model = source(source_name, model_name) -%}
    {%- else -%}
        {%- do exceptions.raise_compiler_error("Invalid model_type provided to stream macro. Use 'model' or 'source'.") -%}
    {%- endif -%}

    {%- if full_refresh -%}
        {%- do log('Target Model: ' ~ target_model, info=true ) -%}
        {{- target_model -}}
    {%- elif stream_name %}
        {{- stream_name -}}
    {%- else %}
        {% do log('Target Model: ' ~ target_model, info=true ) -%}
        {{- target_model -}}
    {%- endif -%}

{%- endmacro -%}
