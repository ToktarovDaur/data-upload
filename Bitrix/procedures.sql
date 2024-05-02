DELETE FROM fact_deals;

insert into fact_deals (id,
	stage_id,
	opportunity,
	lead_id,
	date_create,
	date_modify,
	category_id,
	utm_source,
	project_id)
select
  distinct fd.id as id,
	stage_id,
	opportunity,
	lead_id,
	date_create,
	date_modify,
	category_id,
	utm_source,
	project_id
from
  fact_deals_tmp fd
inner join 
(
  select
    id,
    MAX(date_modify) as max_dt,
    MAX(insert_date) as max_insert
  from
    fact_deals_tmp
  group by
    id) as last_st on
  fd.id = last_st.id
  and fd.date_modify = last_st.max_dt
  and fd.insert_date = last_st.max_insert;


  
DELETE FROM fact_leads;

insert into fact_leads (id,
	title,
	"name",
	second_name,
	last_name,
	company_title,
	company_id,
	contact_id,
	is_return_customer,
	birthdate,
	status_id,
	status_description,
	"comments",
	opportunity,
	is_manual_opportunity,
	assigned_by_id,
	created_by_id,
	modify_by_id,
	date_create,
	date_modify,
	date_closed,
	opened,
	utm_source,
	utm_medium,
	utm_campaign,
	utm_content,
	utm_term,
	project_id)
select distinct fd.id as id,
	title,
	"name",
	second_name,
	last_name,
	company_title,
	company_id,
	contact_id,
	is_return_customer,
	birthdate,
	status_id,
	status_description,
	"comments",
	opportunity,
	is_manual_opportunity,
	assigned_by_id,
	created_by_id,
	modify_by_id,
	date_create,
	date_modify,
	date_closed,
	opened,
	utm_source,
	utm_medium,
	utm_campaign,
	utm_content,
	utm_term,
	project_id
from
  fact_leads_tmp fd
inner join 
(
  select
    id,
    MAX(date_modify) as max_dt,
    MAX(insert_date) as max_insert
  from
    fact_leads_tmp
  group by
    id) as last_st on
  fd.id = last_st.id
  and fd.date_modify = last_st.max_dt
  and fd.insert_date = last_st.max_insert;


DELETE FROM deals_stage_history;

INSERT INTO deals_stage_history (id, type_id, owner_id, created_time, category_id, stage_semantic_id, stage_id, project_id)
SELECT DISTINCT id, type_id, owner_id, created_time, category_id, stage_semantic_id, stage_id, project_id
FROM deals_stage_history_tmp;