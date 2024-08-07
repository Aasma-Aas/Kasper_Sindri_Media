{% set site = var('site') %}
{% set event_name  = var('event_action') %}
{% set db_name = var('db_name_default') %}
{% set tag_filter = var('tag_filter') %}
{% set status_ = var('tag_filter')['status']%}
{% set query_tag = var('tag_filter')['query_tag']%}

{% if event_name == 'Next Click' %}
    {{ Next_Click(site, db_name, status_, query_tag, tag_filter, event_name) }}
{% elif event_name == 'Subscription' %}
    {{ Subscription(site, db_name, event_name) }}
{% endif %}
