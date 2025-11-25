with monthly_revenue as (
select d.department, extract(year from b.bill_date) as bill_year, extract(month from b.bill_month,
sum(b.total_amount) as monthly_revenue,
Lag(sum(b.total_amount)) over (partition by d.department order by extract(year from b.bill_date), extract(month from b.bill_date)) as prev_month_revenue from billing b
join appointment a on b.appointment_id = a.appointment_id
join doctors d on a.doctor_id = d.doctor_id
where b.bill_date >= add_months(trunc(sysdate, 'mm'), -24) group by d.department, extract(year from b.bill_date), extract(month from b.bill_date))
select department, bill_year, bill_month, monthly_revenue, prev_month_revenue, round(((monthly_revenue - prev_month_revenue)/prev_month_revenue) * 100, 2) as growth_percentage from monthly_revenue order by department, bill_year desc, bill_month desc;
