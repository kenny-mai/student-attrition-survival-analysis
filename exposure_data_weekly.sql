-- exposure data
with base as (select dwh_scholar_id
                   , sa_scholar_id
                   , case
                         when ((last_day_at_success_academy != '2024-09-06'
                             and exit_reason_description != 'WITHDRAWAL: Regulated Withdrawal')
                             and withdrawn = 'yes' and attrition_status = 'Intra Year') then 1
                         else 0 end as attrited
                   , case
                         when (attrition_status = 'Retained'
                             and last_day_at_success_academy is not null) then 1
                         else 0 end as fail_to_yield
                   , n              AS time_interval
              from prod_scholar_enrollment_and_attrition.prod_scholar_enrollment_and_attrition_oct psaao
                       cross join (select 0 as n
                                   union all
                                   select 1
                                   union all
                                   select 2
                                   union all
                                   select 3
                                   union all
                                   select 4
                                   union all
                                   select 5
                                   union all
                                   select 6
                                   union all
                                   select 7
                                   union all
                                   select 8
                                   union all
                                   select 9
                                   union all
                                   select 10
                                   union all
                                   select 11
                                   union all
                                   select 12)
              where scholar_grade = 'K'
                and ispreregistered = 'No'
                and attrition_status is not null
                and fail_to_yield = 0
              order by attrited desc, last_day_at_success_academy desc)
   , fact_daily_scholar_status as (select date_key
                                        , sa_scholar_id
                                        , dwh_scholar_id
                                        , ell_status
                                        --, sped_status
                                        , frpl_status
                                        , attendance_status
                                        , excused
                                        , getdate()::date as run_date
                                   from sacs.fact_daily_scholar_status fdss
                                   where attendance_status != 'N/A'
                                     and school_yr = '2024-2025'
                                     and in_session = True
                                     and sa_scholar_id in (select sa_scholar_id from base))
   , attendance as (select sa_scholar_id
                         , dwh_scholar_id
                         , sum(case when attendance_status in ('P') then 1 else 0 end)                       as present_days
                         , sum(case when attendance_status in ('T') then 1 else 0 end)                       as tardy_days
                         , sum(case when attendance_status in ('T', 'P') Then 1 else 0 end)                  as total_for_tardy
                         , sum(case when attendance_status in ('A') and excused = 'False' then 1 else 0 end) as absent_days
                         , sum(case when attendance_status in ('A') and excused = 'True' then 1 else 0 end)  as excused_absent_days
                         , tardy_days + present_days + excused_absent_days + absent_days                     as total_days
                         , case
                               when total_for_tardy > 0 then round(tardy_days::float / total_for_tardy::float, 4)
                               else 0 end                                                                    as tardy_percent
                         , case
                               when total_days > 0 then round(absent_days::float / total_days::float, 4)
                               else 0 end                                                                    as absent_percent
                         , 0                                                                                 as time_interval
                    from fact_daily_scholar_status
                    where date_key = '2024-08-26'
                    group by 1, 2
                        union all
                        select sa_scholar_id
                         , dwh_scholar_id
                         , sum(case when attendance_status in ('P') then 1 else 0 end)                       as present_days
                         , sum(case when attendance_status in ('T') then 1 else 0 end)                       as tardy_days
                         , sum(case when attendance_status in ('T', 'P') Then 1 else 0 end)                  as total_for_tardy
                         , sum(case when attendance_status in ('A') and excused = 'False' then 1 else 0 end) as absent_days
                         , sum(case when attendance_status in ('A') and excused = 'True' then 1 else 0 end)  as excused_absent_days
                         , tardy_days + present_days + excused_absent_days + absent_days                     as total_days
                         , case
                               when total_for_tardy > 0 then round(tardy_days::float / total_for_tardy::float, 4)
                               else 0 end                                                                    as tardy_percent
                         , case
                               when total_days > 0 then round(absent_days::float / total_days::float, 4)
                               else 0 end                                                                    as absent_percent
                         , 1                                                                                 as time_interval
                    from fact_daily_scholar_status
                    where date_key > '2024-08-26'
                      and date_key <= '2024-09-02'
                    group by 1, 2
                    union all
                    select sa_scholar_id
                         , dwh_scholar_id
                         , sum(case when attendance_status in ('P') then 1 else 0 end)                       as present_days
                         , sum(case when attendance_status in ('T') then 1 else 0 end)                       as tardy_days
                         , sum(case when attendance_status in ('T', 'P') Then 1 else 0 end)                  as total_for_tardy
                         , sum(case when attendance_status in ('A') and excused = 'False' then 1 else 0 end) as absent_days
                         , sum(case when attendance_status in ('A') and excused = 'True' then 1 else 0 end)  as excused_absent_days
                         , tardy_days + present_days + excused_absent_days + absent_days                     as total_days
                         , case
                               when total_for_tardy > 0 then round(tardy_days::float / total_for_tardy::float, 4)
                               else 0 end                                                                    as tardy_percent
                         , case
                               when total_days > 0 then round(absent_days::float / total_days::float, 4)
                               else 0 end                                                                    as absent_percent
                         , 2                                                                                 as time_interval
                    from fact_daily_scholar_status
                    where date_key > '2024-09-02'
                      and date_key <= '2024-09-09'
                    group by 1, 2
                    union all
                    select sa_scholar_id
                         , dwh_scholar_id
                         , sum(case when attendance_status in ('P') then 1 else 0 end)                       as present_days
                         , sum(case when attendance_status in ('T') then 1 else 0 end)                       as tardy_days
                         , sum(case when attendance_status in ('T', 'P') Then 1 else 0 end)                  as total_for_tardy
                         , sum(case when attendance_status in ('A') and excused = 'False' then 1 else 0 end) as absent_days
                         , sum(case when attendance_status in ('A') and excused = 'True' then 1 else 0 end)  as excused_absent_days
                         , tardy_days + present_days + excused_absent_days + absent_days                     as total_days
                         , case
                               when total_for_tardy > 0 then round(tardy_days::float / total_for_tardy::float, 4)
                               else 0 end                                                                    as tardy_percent
                         , case
                               when total_days > 0 then round(absent_days::float / total_days::float, 4)
                               else 0 end                                                                    as absent_percent
                         , 3                                                                                 as time_interval
                    from fact_daily_scholar_status
                    where date_key > '2024-09-09'
                      and date_key <= '2024-09-16'
                    group by 1, 2
                    union all
                    select sa_scholar_id
                         , dwh_scholar_id
                         , sum(case when attendance_status in ('P') then 1 else 0 end)                       as present_days
                         , sum(case when attendance_status in ('T') then 1 else 0 end)                       as tardy_days
                         , sum(case when attendance_status in ('T', 'P') Then 1 else 0 end)                  as total_for_tardy
                         , sum(case when attendance_status in ('A') and excused = 'False' then 1 else 0 end) as absent_days
                         , sum(case when attendance_status in ('A') and excused = 'True' then 1 else 0 end)  as excused_absent_days
                         , tardy_days + present_days + excused_absent_days + absent_days                     as total_days
                         , case
                               when total_for_tardy > 0 then round(tardy_days::float / total_for_tardy::float, 4)
                               else 0 end                                                                    as tardy_percent
                         , case
                               when total_days > 0 then round(absent_days::float / total_days::float, 4)
                               else 0 end                                                                    as absent_percent
                         , 4                                                                                 as time_interval
                    from fact_daily_scholar_status
                    where date_key > '2024-09-16'
                      and date_key <= '2024-09-23'
                    group by 1, 2
                    union all
                    select sa_scholar_id
                         , dwh_scholar_id
                         , sum(case when attendance_status in ('P') then 1 else 0 end)                       as present_days
                         , sum(case when attendance_status in ('T') then 1 else 0 end)                       as tardy_days
                         , sum(case when attendance_status in ('T', 'P') Then 1 else 0 end)                  as total_for_tardy
                         , sum(case when attendance_status in ('A') and excused = 'False' then 1 else 0 end) as absent_days
                         , sum(case when attendance_status in ('A') and excused = 'True' then 1 else 0 end)  as excused_absent_days
                         , tardy_days + present_days + excused_absent_days + absent_days                     as total_days
                         , case
                               when total_for_tardy > 0 then round(tardy_days::float / total_for_tardy::float, 4)
                               else 0 end                                                                    as tardy_percent
                         , case
                               when total_days > 0 then round(absent_days::float / total_days::float, 4)
                               else 0 end                                                                    as absent_percent
                         , 5                                                                                 as time_interval
                    from fact_daily_scholar_status
                    where date_key > '2024-09-23'
                      and date_key <= '2024-09-30'
                    group by 1, 2
                    union all
                    select sa_scholar_id
                         , dwh_scholar_id
                         , sum(case when attendance_status in ('P') then 1 else 0 end)                       as present_days
                         , sum(case when attendance_status in ('T') then 1 else 0 end)                       as tardy_days
                         , sum(case when attendance_status in ('T', 'P') Then 1 else 0 end)                  as total_for_tardy
                         , sum(case when attendance_status in ('A') and excused = 'False' then 1 else 0 end) as absent_days
                         , sum(case when attendance_status in ('A') and excused = 'True' then 1 else 0 end)  as excused_absent_days
                         , tardy_days + present_days + excused_absent_days + absent_days                     as total_days
                         , case
                               when total_for_tardy > 0 then round(tardy_days::float / total_for_tardy::float, 4)
                               else 0 end                                                                    as tardy_percent
                         , case
                               when total_days > 0 then round(absent_days::float / total_days::float, 4)
                               else 0 end                                                                    as absent_percent
                         , 6                                                                                 as time_interval
                    from fact_daily_scholar_status
                    where date_key > '2024-09-30'
                      and date_key <= '2024-10-07'
                    group by 1, 2
                    union all
                    select sa_scholar_id
                         , dwh_scholar_id
                         , sum(case when attendance_status in ('P') then 1 else 0 end)                       as present_days
                         , sum(case when attendance_status in ('T') then 1 else 0 end)                       as tardy_days
                         , sum(case when attendance_status in ('T', 'P') Then 1 else 0 end)                  as total_for_tardy
                         , sum(case when attendance_status in ('A') and excused = 'False' then 1 else 0 end) as absent_days
                         , sum(case when attendance_status in ('A') and excused = 'True' then 1 else 0 end)  as excused_absent_days
                         , tardy_days + present_days + excused_absent_days + absent_days                     as total_days
                         , case
                               when total_for_tardy > 0 then round(tardy_days::float / total_for_tardy::float, 4)
                               else 0 end                                                                    as tardy_percent
                         , case
                               when total_days > 0 then round(absent_days::float / total_days::float, 4)
                               else 0 end                                                                    as absent_percent
                         , 7                                                                                 as time_interval
                    from fact_daily_scholar_status
                    where date_key > '2024-10-07'
                      and date_key <= '2024-10-14'
                    group by 1, 2
                    union all
                    select sa_scholar_id
                         , dwh_scholar_id
                         , sum(case when attendance_status in ('P') then 1 else 0 end)                       as present_days
                         , sum(case when attendance_status in ('T') then 1 else 0 end)                       as tardy_days
                         , sum(case when attendance_status in ('T', 'P') Then 1 else 0 end)                  as total_for_tardy
                         , sum(case when attendance_status in ('A') and excused = 'False' then 1 else 0 end) as absent_days
                         , sum(case when attendance_status in ('A') and excused = 'True' then 1 else 0 end)  as excused_absent_days
                         , tardy_days + present_days + excused_absent_days + absent_days                     as total_days
                         , case
                               when total_for_tardy > 0 then round(tardy_days::float / total_for_tardy::float, 4)
                               else 0 end                                                                    as tardy_percent
                         , case
                               when total_days > 0 then round(absent_days::float / total_days::float, 4)
                               else 0 end                                                                    as absent_percent
                         , 8                                                                                 as time_interval
                    from fact_daily_scholar_status
                    where date_key > '2024-10-14'
                      and date_key <= '2024-10-21'
                    group by 1, 2
                    union all
                    select sa_scholar_id
                         , dwh_scholar_id
                         , sum(case when attendance_status in ('P') then 1 else 0 end)                       as present_days
                         , sum(case when attendance_status in ('T') then 1 else 0 end)                       as tardy_days
                         , sum(case when attendance_status in ('T', 'P') Then 1 else 0 end)                  as total_for_tardy
                         , sum(case when attendance_status in ('A') and excused = 'False' then 1 else 0 end) as absent_days
                         , sum(case when attendance_status in ('A') and excused = 'True' then 1 else 0 end)  as excused_absent_days
                         , tardy_days + present_days + excused_absent_days + absent_days                     as total_days
                         , case
                               when total_for_tardy > 0 then round(tardy_days::float / total_for_tardy::float, 4)
                               else 0 end                                                                    as tardy_percent
                         , case
                               when total_days > 0 then round(absent_days::float / total_days::float, 4)
                               else 0 end                                                                    as absent_percent
                         , 9                                                                                 as time_interval
                    from fact_daily_scholar_status
                    where date_key > '2024-10-21'
                      and date_key <= '2024-10-28'
                    group by 1, 2
                    union all
                    select sa_scholar_id
                         , dwh_scholar_id
                         , sum(case when attendance_status in ('P') then 1 else 0 end)                       as present_days
                         , sum(case when attendance_status in ('T') then 1 else 0 end)                       as tardy_days
                         , sum(case when attendance_status in ('T', 'P') Then 1 else 0 end)                  as total_for_tardy
                         , sum(case when attendance_status in ('A') and excused = 'False' then 1 else 0 end) as absent_days
                         , sum(case when attendance_status in ('A') and excused = 'True' then 1 else 0 end)  as excused_absent_days
                         , tardy_days + present_days + excused_absent_days + absent_days                     as total_days
                         , case
                               when total_for_tardy > 0 then round(tardy_days::float / total_for_tardy::float, 4)
                               else 0 end                                                                    as tardy_percent
                         , case
                               when total_days > 0 then round(absent_days::float / total_days::float, 4)
                               else 0 end                                                                    as absent_percent
                         , 10                                                                                 as time_interval
                    from fact_daily_scholar_status
                    where date_key > '2024-10-28'
                      and date_key <= '2024-11-04'
                    group by 1, 2
                    union all
                    select sa_scholar_id
                         , dwh_scholar_id
                         , sum(case when attendance_status in ('P') then 1 else 0 end)                       as present_days
                         , sum(case when attendance_status in ('T') then 1 else 0 end)                       as tardy_days
                         , sum(case when attendance_status in ('T', 'P') Then 1 else 0 end)                  as total_for_tardy
                         , sum(case when attendance_status in ('A') and excused = 'False' then 1 else 0 end) as absent_days
                         , sum(case when attendance_status in ('A') and excused = 'True' then 1 else 0 end)  as excused_absent_days
                         , tardy_days + present_days + excused_absent_days + absent_days                     as total_days
                         , case
                               when total_for_tardy > 0 then round(tardy_days::float / total_for_tardy::float, 4)
                               else 0 end                                                                    as tardy_percent
                         , case
                               when total_days > 0 then round(absent_days::float / total_days::float, 4)
                               else 0 end                                                                    as absent_percent
                         , 11                                                                                 as time_interval
                    from fact_daily_scholar_status
                    where date_key > '2024-11-04'
                      and date_key <= '2024-11-11'
                    group by 1, 2
                    union all
                    select sa_scholar_id
                         , dwh_scholar_id
                         , sum(case when attendance_status in ('P') then 1 else 0 end)                       as present_days
                         , sum(case when attendance_status in ('T') then 1 else 0 end)                       as tardy_days
                         , sum(case when attendance_status in ('T', 'P') Then 1 else 0 end)                  as total_for_tardy
                         , sum(case when attendance_status in ('A') and excused = 'False' then 1 else 0 end) as absent_days
                         , sum(case when attendance_status in ('A') and excused = 'True' then 1 else 0 end)  as excused_absent_days
                         , tardy_days + present_days + excused_absent_days + absent_days                     as total_days
                         , case
                               when total_for_tardy > 0 then round(tardy_days::float / total_for_tardy::float, 4)
                               else 0 end                                                                    as tardy_percent
                         , case
                               when total_days > 0 then round(absent_days::float / total_days::float, 4)
                               else 0 end                                                                    as absent_percent
                         , 12                                                                                 as time_interval
                    from fact_daily_scholar_status
                    where date_key > '2024-11-11'
                      and date_key <= '2024-11-18'
                    group by 1, 2)
   , reprimands as (select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_rep
                         , 0                           as time_interval
                    from sacs.fact_suspension fs1
                    where incidenttype_nm like 'REPRIMAND%'
                      and incident_dt = '2024-08-26'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_rep
                         , 1                           as time_interval
                    from sacs.fact_suspension fs1
                    where incidenttype_nm like 'REPRIMAND%'
                      and incident_dt > '2024-08-26'
                      and incident_dt <= '2024-09-02'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_rep
                         , 2                           as time_interval
                    from sacs.fact_suspension fs1
                    where incidenttype_nm like 'REPRIMAND%'
                      and incident_dt > '2024-09-02'
                      and incident_dt <= '2024-09-09'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_rep
                         , 3                           as time_interval
                    from sacs.fact_suspension fs1
                    where incidenttype_nm like 'REPRIMAND%'
                      and incident_dt > '2024-09-09'
                      and incident_dt <= '2024-09-16'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_rep
                         , 4                           as time_interval
                    from sacs.fact_suspension fs1
                    where incidenttype_nm like 'REPRIMAND%'
                      and incident_dt > '2024-09-16'
                      and incident_dt <= '2024-09-23'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_rep
                         , 5                           as time_interval
                    from sacs.fact_suspension fs1
                    where incidenttype_nm like 'REPRIMAND%'
                      and incident_dt > '2024-09-23'
                      and incident_dt <= '2024-09-30'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_rep
                         , 6                           as time_interval
                    from sacs.fact_suspension fs1
                    where incidenttype_nm like 'REPRIMAND%'
                      and incident_dt > '2024-09-30'
                      and incident_dt <= '2024-10-07'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_rep
                         , 7                           as time_interval
                    from sacs.fact_suspension fs1
                    where incidenttype_nm like 'REPRIMAND%'
                      and incident_dt > '2024-10-07'
                      and incident_dt <= '2024-10-14'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_rep
                         , 8                           as time_interval
                    from sacs.fact_suspension fs1
                    where incidenttype_nm like 'REPRIMAND%'
                      and incident_dt > '2024-10-14'
                      and incident_dt <= '2024-10-21'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_rep
                         , 9                           as time_interval
                    from sacs.fact_suspension fs1
                    where incidenttype_nm like 'REPRIMAND%'
                      and incident_dt > '2024-10-21'
                      and incident_dt <= '2024-10-28'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_rep
                         , 10                           as time_interval
                    from sacs.fact_suspension fs1
                    where incidenttype_nm like 'REPRIMAND%'
                      and incident_dt > '2024-10-28'
                      and incident_dt <= '2024-11-04'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_rep
                         , 11                           as time_interval
                    from sacs.fact_suspension fs1
                    where incidenttype_nm like 'REPRIMAND%'
                      and incident_dt > '2024-11-04'
                      and incident_dt <= '2024-11-11'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_rep
                         , 12                           as time_interval
                    from sacs.fact_suspension fs1
                    where incidenttype_nm like 'REPRIMAND%'
                      and incident_dt > '2024-11-11'
                      and incident_dt <= '2024-11-18'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1)
   , suspensions as (select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_sus
                         , 0                           as time_interval
                    from sacs.fact_suspension fs2
                    where incidenttype_nm like 'SUSPENSION%'
                      and incident_dt = '2024-08-26'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_sus
                         , 1                           as time_interval
                    from sacs.fact_suspension fs2
                    where incidenttype_nm like 'SUSPENSION%'
                      and incident_dt > '2024-08-26'
                      and incident_dt <= '2024-09-02'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_sus
                         , 2                           as time_interval
                    from sacs.fact_suspension fs2
                    where incidenttype_nm like 'SUSPENSION%'
                      and incident_dt > '2024-09-02'
                      and incident_dt <= '2024-09-09'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_sus
                         , 3                           as time_interval
                    from sacs.fact_suspension fs2
                    where incidenttype_nm like 'SUSPENSION%'
                      and incident_dt > '2024-09-09'
                      and incident_dt <= '2024-09-16'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_sus
                         , 4                           as time_interval
                    from sacs.fact_suspension fs2
                    where incidenttype_nm like 'SUSPENSION%'
                      and incident_dt > '2024-09-16'
                      and incident_dt <= '2024-09-23'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_sus
                         , 5                           as time_interval
                    from sacs.fact_suspension fs2
                    where incidenttype_nm like 'SUSPENSION%'
                      and incident_dt > '2024-09-23'
                      and incident_dt <= '2024-09-30'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_sus
                         , 6                           as time_interval
                    from sacs.fact_suspension fs2
                    where incidenttype_nm like 'SUSPENSION%'
                      and incident_dt > '2024-09-30'
                      and incident_dt <= '2024-10-07'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_sus
                         , 7                           as time_interval
                    from sacs.fact_suspension fs2
                    where incidenttype_nm like 'SUSPENSION%'
                      and incident_dt > '2024-10-07'
                      and incident_dt <= '2024-10-14'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_sus
                         , 8                           as time_interval
                    from sacs.fact_suspension fs2
                    where incidenttype_nm like 'SUSPENSION%'
                      and incident_dt > '2024-10-14'
                      and incident_dt <= '2024-10-21'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_sus
                         , 9                           as time_interval
                    from sacs.fact_suspension fs2
                    where incidenttype_nm like 'SUSPENSION%'
                      and incident_dt > '2024-10-21'
                      and incident_dt <= '2024-10-28'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_sus
                         , 10                           as time_interval
                    from sacs.fact_suspension fs2
                    where incidenttype_nm like 'SUSPENSION%'
                      and incident_dt > '2024-10-28'
                      and incident_dt <= '2024-11-04'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_sus
                         , 11                           as time_interval
                    from sacs.fact_suspension fs2
                    where incidenttype_nm like 'SUSPENSION%'
                      and incident_dt > '2024-11-04'
                      and incident_dt <= '2024-11-11'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1
                    union all
                    select idnumber                    as sa_scholar_id
                         , count(distinct incident_id) as count_sus
                         , 12                           as time_interval
                    from sacs.fact_suspension fs2
                    where incidenttype_nm like 'SUSPENSION%'
                      and incident_dt > '2024-11-11'
                      and incident_dt <= '2024-11-18'
                      and sa_scholar_id in (select sa_scholar_id from base)
                    group by 1)
   , final as (select base.sa_scholar_id
                    , base.dwh_scholar_id
                    , base.time_interval
                    , tardy_days
                    , absent_days
                    , case when absent_days is null then null else (case when count_rep is null then 0 else count_rep end) end as total_rep
                    , case when absent_days is null then null else (case when count_sus is null then 0 else count_sus end) end as total_sus
               from base
                        left join attendance as att
                                  on base.sa_scholar_id = att.sa_scholar_id
                                      and base.time_interval = att.time_interval
                        left join reprimands as fs1
                                  on base.sa_scholar_id = fs1.sa_scholar_id
                                      and base.time_interval = fs1.time_interval
                        left join suspensions as fs2
                                  on base.sa_scholar_id = fs2.sa_scholar_id
                                      and base.time_interval = fs2.time_interval
               where tardy_days is not null
               order by sa_scholar_id, time_interval)
select *
from final