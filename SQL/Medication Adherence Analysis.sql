select p.patient_id, p.first_name, p.last_name, pr.medication_name, pr.end_date as last_refill_date, case
when pr.end_date < sysdate - 30 then 'Critical Non-Adherence'
when pr.end_date < sysdate - 15 then 'Moderate Non-Adherence'
else 'Within Guidelines' end as adherence_status, pr.refills_remanining from patients p 
join medical_records mr on p.patient_id = mr.patient_id'
join prescriptions pr on mr.record_id = pr.record_id
where pr.medication_name in ('Insulin', 'Warfarin', 'Levothyroxine', 'Metformin) and pr.refills_remaining > 0  and pre.end_date < sysdate - 7 order by adherence_status , pr.end_Date;