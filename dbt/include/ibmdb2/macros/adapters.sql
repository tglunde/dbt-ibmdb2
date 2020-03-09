{% macro ibmdb2__list_relations_without_caching(information_schema, schema) %}
    {% call statement('list_relations_without_caching', fetch_result=True) -%}
        select
          lower(table_catalog) as "database",
          lower(table_name) as "name",
          lower(table_schema) as "schema",
      	  lower(table_type) as "table_type"
        from (
    		select table_catalog, table_name,table_schema,(CASE table_type WHEN 'VIEW' THEN 'view' ELSE 'table' END) as table_type  from SYSIBM.TABLES
    	  )
        where upper(table_schema) = '{{ schema |upper }}'
    {% endcall %}  
    {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro ibmdb2__list_schemas(database) -%}
    {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
        select distinct schemaname from SYSCAT.SCHEMATA
    {% endcall %}
    {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro ibmdb2__check_schema_exists(database, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
    select count(*) as schema_exist from (
		select schemaname  from SYSCAT.SCHEMATA
    ) WHERE upper(schemaname) = '{{ schema | upper }}'
    {{ log(schemaname ~ "  -  "*20)}}
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro ibmdb2__create_schema(database_name, schema_name) -%} 
    {{ log(schema_name ~ " _ _ _"*15)}}
    {%if not adapter.check_schema_exists(database_name,schema_name) %}
        {% call statement('create_schema', fetch_result=True, auto_begin=False) -%}
            CREATE SCHEMA {{ schema_name }}
        {% endcall %}
    {%- endif -%}
{% endmacro %}

{% macro ibmdb2__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }} 
  {%- endcall %}
{% endmacro %}

{% macro ibmdb2__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create table
    {{ relation }}
  as (
    {{ sql }}
  ) with data;
{% endmacro %}

{% macro ibmdb2__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  
  create or replace view {{ relation }} as 
    {{ sql }}
  ;
{% endmacro %}

{% macro ibmdb2__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    RENAME table {{ from_relation.schema }}.{{ from_relation.identifier }} TO {{ to_relation.identifier }}
  {%- endcall %}
{% endmacro %}

{% macro ibmdb2__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          lower(column_name) as column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale
      from SYSIBM.columns
      where upper(table_name) = '{{ relation.identifier|upper }}'
        and upper(table_schema) = '{{ relation.schema|upper }}'
      order by ordinal_position

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro ibmdb2__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation | replace('"', '') }} immediate
  {%- endcall %}
{% endmacro %}

{% macro ibmdb2__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    BEGIN    
    IF EXISTS (SELECT 1 FROM SYSCAT.TABLES WHERE TABSCHEMA = '{{relation.schema | upper}}' AND TABNAME = '{{ relation.name |upper }}') THEN           
      PREPARE stmt FROM 'DROP {{relation.type}} {{relation | replace ('"', '')}}';
      EXECUTE stmt;                            
    END IF;                                                
    END;
  {%- endcall %}
{% endmacro %}