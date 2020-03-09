{% materialization view, adapter='ibmdb2' -%}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}