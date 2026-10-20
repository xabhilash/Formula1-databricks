-- Databricks notebook source
create or replace view formula1.gold.v_driver_standing as
with driver_session_summary as
(select r.season, d.driver_id, d.driver_name, d.nationality, count(*) as race_starts, sum(r.points) as total_points, count_if(r.is_win) as number_of_wins, count_if(r.is_podium) as number_of_podiums 
from formula1.gold.fact_session_results r
        join formula1.gold.dim_drivers d
        on r.driver_id = d.driver_id
group by r.season, d.driver_id, d.driver_name, d.nationality)
select season, driver_id, driver_name, nationality, rank() over(partition by season order by total_points desc, number_of_wins desc) as standing, race_starts, total_points, number_of_wins, number_of_podiums
from driver_session_summary

-- COMMAND ----------

select * from formula1.gold.v_driver_standing where season = 2025

-- COMMAND ----------

