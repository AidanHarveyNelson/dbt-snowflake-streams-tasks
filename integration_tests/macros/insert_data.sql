{% macro insert_data() %}

    {% set test_table  = [target.database, target.schema, 'testing']|join('.') %}

    {% set insert_data_sql %}
        begin;
        insert into {{test_table}} values (1, 'test data');
        commit;
    {% endset %}

    {% do log('Inserting data into testing table ' ~ test_table, info = true) %}
    {% do run_query(insert_data_sql) %}

{% endmacro %}
