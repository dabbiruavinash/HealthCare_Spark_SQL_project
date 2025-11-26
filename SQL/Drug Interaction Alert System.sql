with patient_medications as (
select p.patient_id, p.first_name,p.last_name,listagg(pr.medication_name, ',') within group (order by pr.medication_name) as current_meds,
count(distinct pr.medication_name) as unique_med_count from patients p
join medical_records mr on p.patient_id = mr.patient_id
join prescriptions pr on mr.record_id = pr.record_id
where pr.end_date >= sysdate and mr.visit_date >= add_months(trunc(sysdate), -3)
group by p.patient_id, p.first_name, p.last_name),

interaction_alerts as (
select pm.*, case
when pm.unique_med_count > 5 then 'Multiple Medication Alert'
when pm.current_meds like '%Warfarin%' and pm.current_meds like '%'Aspirin%' then 'Blood thinner interaction'
when pm.current_meds like '%ACE Inhibitor%' and pm.current_meds like '%Potassium%' then 'Electrolyte Interaction'
when pm.unique_med_count >= 3 then 'Polypharmacy Risk' else 'No critical interactions' end as interaction_alert from patient_medications pm where pm.unique_med_count >= 2)

select * from interaction_alert where interaction_alert != 'No critical interactions' order by unqiue_med_count desc;