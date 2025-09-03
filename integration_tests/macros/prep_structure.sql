{% macro prep_structure() %}

    {% set create_schema_sql = "CREATE SCHEMA IF NOT EXISTS " ~ target.schema %}

    {% do log('Creating schema ' ~ target.schema, info = true) %}
    {% do run_query(create_schema_sql) %}

    {% set test_table  = [
        [target.database, target.schema, 'testing_source']|join('.'),
        [target.database, target.schema, 'testing_ref']|join('.'),
    ] %}

    {% for table in test_table %}
        {% set create_testing_table_sql %}
            begin;
            create or replace table {{ table }} (
                id integer,
                data string
            );
            commit;
        {% endset %}

        {% do log('Creating testing table ' ~ table, info = true) %}
        {% do run_query(create_testing_table_sql) %}
    {% endfor %}

{% endmacro %}
