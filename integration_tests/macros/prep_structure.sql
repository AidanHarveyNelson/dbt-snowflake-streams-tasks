{% macro prep_structure() %}

    {% set test_table  = [target.database, target.schema, 'testing']|join('.') %}

    {% set create_schema_sql = "CREATE SCHEMA IF NOT EXISTS " ~ target.schema %}

    {% set create_testing_table_sql %}
        begin;
        create or replace table {{ test_table }} (
            id integer,
            data string
        );
        commit;
    {% endset %}

    {% do log('Creating schema ' ~ target.schema, info = true) %}
    {% do run_query(create_schema_sql) %}

    {% do log('Creating testing table ' ~ test_table, info = true) %}
    {% do run_query(create_testing_table_sql) %}

{% endmacro %}
