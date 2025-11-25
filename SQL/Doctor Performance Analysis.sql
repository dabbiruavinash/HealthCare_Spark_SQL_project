with doctor_stats as (
select d.doctor_id, d.first_name, d.last_name, d.specialization, count(Distinct mr.record_id) as total_visits, avg(case when mr.follow_up_requried = 'Y' then 1 else 0 end) as readmission_rate, count(Distinct case when mr.visit_date >= mr.follow_up_date - 30 then mr.patient_id end) as timely_follow_ups from doctors d join medical_records mr on d.doctor_id = mr.doctor_id where mr.visit_date >= ADD_months(trunc(sysdate), -6) group by d.doctor_id, d.first_name, d.last_name, d.specialization),
ranked_doctors as (
select *, rank() over (order by timely_follow_ups desc, readmission_rate asc) as performance_rank from doctor_stats where total_visit >= 10)
selct * from ranked_doctors where performance_rank <= 10;
