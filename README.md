# mes-requ-tes-hive

#06-10-15

set APPX_COUNT_DISTINCT=true;

create table hadoop_temp.jy_passages_par_mois as

select concat(get_json_object(visites.visite,'$.contractIdentifier.code'),'-',get_json_object(visites.visite,'$.contractIdentifier.num')) as id_visites,
count(distinct CASE WHEN date between '2015-04-01' and '2015-05-01' then concat(get_json_object(visites.visite,'$.contractIdentifier.code'),'-',get_json_object(visites.visite,'$.contractIdentifier.num')) end) AS nb_passages_avril,
count(distinct CASE WHEN date between '2015-05-01' and '2015-06-01' then concat(get_json_object(visites.visite,'$.contractIdentifier.code'),'-',get_json_object(visites.visite,'$.contractIdentifier.num')) end) AS nb_passages_mai,
count(distinct CASE WHEN date between '2015-06-01' and '2015-07-01' then concat(get_json_object(visites.visite,'$.contractIdentifier.code'),'-',get_json_object(visites.visite,'$.contractIdentifier.num')) end) AS nb_passages_juin
from eframe.visites
where concat(get_json_object(visites.visite,'$.contractIdentifier.code'),'-',get_json_object(visites.visite,'$.contractIdentifier.num')) is not null
and date between '2015-04-01' and '2015-07-01'
group by concat(get_json_object(visites.visite,'$.contractIdentifier.code'),'-',get_json_object(visites.visite,'$.contractIdentifier.num'))


set APPX_COUNT_DISTINCT=true;
create table hadoop_temp.jy_appels_par_mois as
select concat(get_json_object(client_activity.visites,'$.contextparams.scs'),'-',get_json_object(client_activity.visites,'$.contextparams.csu')) as id_client,
count(distinct CASE WHEN date between '2015-04-01' and '2015-05-01' then get_json_object(client_activity.visites,'$.jsessionid') end) AS nb_appels_avril,
count(distinct CASE WHEN date between '2015-05-01' and '2015-06-01' then get_json_object(client_activity.visites,'$.jsessionid') end) AS nb_appels_mai,
count(distinct CASE WHEN date between '2015-06-01' and '2015-07-01' then get_json_object(client_activity.visites,'$.jsessionid') end) AS nb_appels_juin
from fast.client_activity
where concat(get_json_object(client_activity.visites,'$.contextparams.csu'),'-',get_json_object(client_activity.visites,'$.contextparams.scs')) is not null
and date between '2015-04-01' and '2015-07-01'
group by concat(get_json_object(client_activity.visites,'$.contextparams.csu'),'-',get_json_object(client_activity.visites,'$.contextparams.scs'))

set APPX_COUNT_DISTINCT=true;


create table hadoop_temp.jy_visites_par_mois as
select 
post_evar4,
count(distinct CASE WHEN date_time between '2015-04-01 00:00:00:0' and '2015-05-01 00:00:00:0' THEN visit_id END) AS nb_visites_avril,
count(distinct CASE WHEN date_time between '2015-05-01 00:00:00:0' and '2015-06-01 00:00:00:0' THEN visit_id END) AS nb_visites_mai,
count(distinct CASE WHEN date_time between '2015-06-01 00:00:00:0' and '2015-07-01 00:00:00:0' THEN visit_id END) AS nb_visites_juin
from omniture.hit_data
where post_evar4 <> ''
and date_time between '2015-04-01 00:00:00:0' and '2015-07-01 00:00:00:0'
group by post_evar4
