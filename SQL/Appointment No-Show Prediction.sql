with appointment_patterns as (
select p.patient_id, p.first_name, p.last_name, count(*) as total_appointments,
count(case when a.status = 'No-show' then 1 end) as no_show_count,
round(count(case when a.status = 'No-show' then 1 end) * 100.0/count(*),2) as no_show_rate,
avg(case when a.status != 'No-show' then extract(hour from a.appointment_date) end) as avg_appointment_hour,
count(distinct to_char(a.appointment_date, 'D')) as day_of_week_visited from patients p 
join appointment_date >= add_months(trunc(sysdate), -12)
group by p.patient_id, p.first_name, p.last_name),

predication_model as (
select *, case 
when no_show_rate > 30 or total appointments < 3 then 'High No-show risk'
when no_show_rate between 15 and 30 then 'Medium No-show risk' else as predication from appointment_pattern)
select * from predication_model where predication != 'Low No-show risk' order by no_show_rate desc;
