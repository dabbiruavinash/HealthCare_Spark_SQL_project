SELECT 
    p.patient_id,
    p.first_name,
    p.last_name,
    i.provider_name,
    i.coverage_type,
    i.deductible,
    i.copay, sum(b.patient_balance) as total_out_of_pocket, case
when i.expiration_date < sysdate then 'coverage expired'
when i.expiration_date < sysdate + 30 then 'expiring soon'
when sum(b.patient_balance) > i.deductible * 0.8 then 'high out-of-pocket' else 'Adequate Coverage' end as coverage_status from patients p
join insurance i on p.patient_id = i.patient_id
left join billing b on p.patient_id = b.patient_id
where b.bill_date >= add_months(trunc(sysdate), -6)
group by p.patient_id, p.first_name, p.last_name, i.provider_name, i.coverage_type, i.deductible, i.copay, i.expiration_date having sum(b.patient_balance) > 0 order by total_out_of_pocket desc;
