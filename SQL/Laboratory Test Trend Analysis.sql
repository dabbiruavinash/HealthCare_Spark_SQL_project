with diabetic_patients as (
select distinct p.patient_id from patients p join medical_records mr on p.patient_id = mr.patient_id where mr.diagnosis like '%Diabetes%'),

test_trends as (
select 
        p.patient_id,
        p.first_name,
        p.last_name,
        lt.test_name,
        EXTRACT(YEAR FROM lt.test_date) as test_year,
        EXTRACT(MONTH FROM lt.test_date) as test_month,
        AVG(TO_NUMBER(REGEXP_SUBSTR(lt.result_value, '^\d+(\.\d+)?'))) as avg_result, COUNT(*) as test_count FROM patients p
    JOIN diabetic_patients dp ON p.patient_id = dp.patient_id
    JOIN medical_records mr ON p.patient_id = mr.patient_id
    JOIN lab_tests lt ON mr.record_id = lt.record_id
    WHERE lt.test_name IN ('HbA1c', 'Blood Glucose', 'Cholesterol')
      AND lt.test_date >= ADD_MONTHS(TRUNC(SYSDATE), -24)
      AND REGEXP_LIKE(lt.result_value, '^\d+(\.\d+)?') GROUP BY p.patient_id, p.first_name, p.last_name, lt.test_name,  EXTRACT(YEAR FROM lt.test_date), EXTRACT(MONTH FROM lt.test_date)),

trend_analysis AS (
SELECT *,LAG(avg_result) OVER (PARTITION BY patient_id, test_name ORDER BY test_year, test_month) as prev_avg,
ROUND(((avg_result - LAG(avg_result) OVER (PARTITION BY patient_id, test_name ORDER BY test_year, test_month)) / 
LAG(avg_result) OVER (PARTITION BY patient_id, test_name ORDER BY test_year, test_month)) * 100, 2) as change_percentage
    FROM test_trends
)
SELECT * FROM trend_analysis
WHERE prev_avg IS NOT NULL
ORDER BY patient_id, test_name, test_year DESC, test_month DESC;