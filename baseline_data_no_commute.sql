-- baseline_data_no_commute

with base as (select dwh_scholar_id
                   , sa_scholar_id
                   , scholar_grade
                   , borough
                   , school_name
                   , school_manager
                   , school_type
                   , first_name
                   , last_name
                   , first_day_at_success_academy
                   , last_day_at_success_academy
                   , exit_reason_description
                   , less_than_20
                   , between_20_60
                   , above_60
                   , case when first_day_at_success_academy > '2024-08-19' then 1 else 0 end as late_enroll
                   , case
                         when ((last_day_at_success_academy != '2024-09-06'
                             and exit_reason_description != 'WITHDRAWAL: Regulated Withdrawal')
                             and withdrawn = 'yes'
                                   and attrition_status = 'Intra Year'
                                   and (last_day_at_success_academy is null or last_day_at_success_academy < '2024-11-18')
                             ) then 1
                         else 0 end                                                          as attrited
                   , case
                         when (attrition_status = 'Retained'
                             and last_day_at_success_academy is not null) then 1
                         else 0 end                                                          as fail_to_yield
              from prod_scholar_enrollment_and_attrition.prod_scholar_enrollment_and_attrition_oct psaao
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
   , detailed_demographics as (select sa_scholar_id
                                    , ell_status
                                    --, sped_status
                                    , frpl_status
                                    , date_key
                                    , run_date
                                    , row_number() over (partition by sa_scholar_id order by date_key desc) as dupcnt_fdss
                               from fact_daily_scholar_status
                               where date_key <= run_date::date
                               qualify dupcnt_fdss = 1)
   , dsoi as (select sa_scholar_id
                   , gender
              from sacs.dim_scholar_other_info)
   , latlongs as (select * from raw_data_science.scholar_lat_long)
   , dim_school as (select *
                    from sacs.dim_school
                    where dwh_school_year_id in (15)
                      and active is true
                      and esd_school_name not ilike ('%summer%')
                      and esd_school_name not ilike ('%train%')
                      and school_type_abbrev in ('ES'))
   , dim_course as (select dc.*,
                           esd_school_name,
                           school_type_abbrev
                    from sacs.dim_course as dc
                             inner join dim_school as ds on
                        dc.dwh_school_year_id = ds.dwh_school_year_id
                            and
                        dc.dwh_school_id = ds.dwh_school_id
                    where course_displayname ilike ('%homeroom grade k%'))
   , fse as (select fse.ioa_student_id,
                    fse.dwh_section_id,
                    null as max_exit_date
             from sacs.fact_section_enrollment as fse
                      inner join dim_course as dc on
                 fse.dwh_school_year_id = dc.dwh_school_year_id
                     and
                 fse.dwh_school_id = dc.dwh_school_id
                     and
                 fse.dwh_course_id = dc.dwh_course_id
             where (section_date_exited is null
                 or section_date_exited > getdate())
               and fse.ioa_student_id in (select sa_scholar_id from base where attrited = 0)
             union all
             select fse.ioa_student_id,
                    fse.dwh_section_id,
                    max(fse.section_date_exited) over (partition by fse.dwh_scholar_id) as max_exit_date
             from sacs.fact_section_enrollment as fse
                      inner join dim_course as dc on
                 fse.dwh_school_year_id = dc.dwh_school_year_id
                     and
                 fse.dwh_school_id = dc.dwh_school_id
                     and
                 fse.dwh_course_id = dc.dwh_course_id
             where fse.ioa_student_id in (select sa_scholar_id from base where attrited = 1)
             group by fse.ioa_student_id, fse.dwh_scholar_id, fse.dwh_section_id, fse.section_date_exited
             qualify fse.section_date_exited = max_exit_date)
   , fte as (select fte.dwh_section_id,
                    fte.wd_employee_number
             from sacs.fact_teacher_enrollment as fte
                      inner join fse on
                 fte.dwh_section_id = fse.dwh_section_id
             where (end_date is null or end_date > getdate())
               and role in ('Teacher of Record'))
   , dim_workforce_refactored as (select dw.*
                                  from sacs.dim_workforce_refactored as dw
                                           inner join (select distinct wd_employee_number from fte) as fte on
                                      dw.worker_id = fte.wd_employee_number)
   , lead_teacher as (select distinct fse.ioa_student_id,
                                      fte.wd_employee_number,
                                      dw.first_name + ' ' + dw.last_name as lead_teacher
                      from fse
                               left join fte on
                          fse.dwh_section_id = fte.dwh_section_id
                               left join dim_workforce_refactored as dw on
                          fte.wd_employee_number = dw.worker_id
                      order by ioa_student_id)
   , final as (select base.sa_scholar_id
                    , base.dwh_scholar_id
                    , borough
                    , school_name
                    , school_manager
                    , lead_teacher
                    , late_enroll
                    , first_day_at_success_academy                     as first_sa_day
                    , last_day_at_success_academy                      as last_sa_day
/*                    , case
                          when last_day_at_success_academy between '2024-08-26' and '2024-09-26' then 1
                          when last_day_at_success_academy between '2024-09-26' and '2024-10-26' then 2
                          when last_day_at_success_academy between '2024-10-26' and '2024-11-26' then 3
                          when last_day_at_success_academy between '2024-11-26' and '2024-12-26' then 4
                          when last_day_at_success_academy between '2024-12-26' and '2025-01-26' then 5
                          when last_day_at_success_academy between '2025-01-26' and '2025-02-26' then 6
                          when last_day_at_success_academy between '2025-02-26' and '2025-03-26' then 7
                          when last_day_at_success_academy between '2025-03-26' and '2025-04-26' then 8
                          else 8 end                                as last_day_recorded*/
                    , case
                          when last_day_at_success_academy between '2024-08-26' and '2024-09-02' then 1
                          when last_day_at_success_academy between '2024-09-02' and '2024-09-09' then 2
                          when last_day_at_success_academy between '2024-09-09' and '2024-09-16' then 3
                          when last_day_at_success_academy between '2024-09-16' and '2024-09-23' then 4
                          when last_day_at_success_academy between '2024-09-23' and '2024-09-30' then 5
                          when last_day_at_success_academy between '2024-09-30' and '2024-10-07' then 6
                          when last_day_at_success_academy between '2024-10-07' and '2024-10-14' then 7
                          when last_day_at_success_academy between '2024-10-14' and '2024-10-21' then 8
                          when last_day_at_success_academy between '2024-10-21' and '2024-10-28' then 9
                          when last_day_at_success_academy between '2024-10-28' and '2024-11-04' then 10
                          when last_day_at_success_academy between '2024-11-04' and '2024-11-11' then 11
                          when last_day_at_success_academy between '2024-11-11' and '2024-11-18' then 12
                          else 12 end                                as last_day_recorded
/*                    , case
                          when last_day_at_success_academy = '2024-08-27' then 1
                          when last_day_at_success_academy = '2024-08-28' then 2
                          when last_day_at_success_academy = '2024-08-29' then 3
                          when last_day_at_success_academy = '2024-08-30' then 4
                          when last_day_at_success_academy = '2024-09-02' then 5
                          when last_day_at_success_academy = '2024-09-03' then 6
                          when last_day_at_success_academy = '2024-09-04' then 7
                          when last_day_at_success_academy = '2024-09-05' then 8
                          when last_day_at_success_academy = '2024-09-06' then 9
                          when last_day_at_success_academy = '2024-09-09' then 10
                          else 10 end                                as last_day_recorded*/
                    , first_name
                    , last_name
                    , attrited
                    , exit_reason_description
                    , case when gender in ('Female') then 1 else 0 end as gender_female
                    , case when ell_status is true then 1 else 0 end   as ell_status
                    , case when frpl_status is true then 1 else 0 end  as frpl_status
                    , less_than_20
                    , between_20_60
                    , above_60
                    , address
                    , case when latitude = '' then null else latitude::float end
                    , case when longitude = '' then null else longitude::float end
               from base
                        left join detailed_demographics as dd
                                  on base.sa_scholar_id = dd.sa_scholar_id
                        left join dsoi
                                  on base.sa_scholar_id = dsoi.sa_scholar_id
                        left join latlongs as ll
                                  on base.sa_scholar_id = ll.sa_scholar_id
                        left join lead_teacher as lt
                                  on base.sa_scholar_id = lt.ioa_student_id
               order by sa_scholar_id, last_day_recorded)
select *
from final