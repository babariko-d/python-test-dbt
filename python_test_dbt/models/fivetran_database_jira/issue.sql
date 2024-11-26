with issue_components as (
select imh.issue_id as issue_id, listagg(component.name, ',') within group(order by name) as components_list from {{ source('fivetran_database_jira', 'ISSUE_MULTISELECT_HISTORY') }} as imh join {{ source('fivetran_database_jira', 'COMPONENT') }} as component on imh.value = component.id where imh.field_id = 'components' and imh.is_active = true and imh.value is not null
group by imh.issue_id
),
issue_with_names as (
select issue.*, reporter.name as reporter_name, assignee.name as assignee_name from {{ source('fivetran_database_jira', 'ISSUE') }} as issue join {{ source('fivetran_database_jira', 'USER') }} as reporter on issue.reporter = reporter.id left join {{ source('fivetran_database_jira', 'USER') }} as assignee on issue.assignee = assignee.id
)
select issue_with_names.*, issue_components.components_list from issue_with_names left join issue_components on issue_with_names.id = issue_components.issue_id
