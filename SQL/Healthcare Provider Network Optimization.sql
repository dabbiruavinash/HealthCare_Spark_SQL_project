WITH patient_travel_patterns AS (
    SELECT 
        p.patient_id,
        p.address as patient_address,
        d.doctor_id,
        d.first_name || ' ' || d.last_name as doctor_name,
        d.department,
        COUNT(*) as visit_count,
        RANK() OVER (PARTITION BY p.patient_id ORDER BY COUNT(*) DESC) as doctor_rank
    FROM patients p
    JOIN appointments a ON p.patient_id = a.patient_id
    JOIN doctors d ON a.doctor_id = d.doctor_id
    WHERE a.appointment_date >= ADD_MONTHS(TRUNC(SYSDATE), -12)
    GROUP BY p.patient_id, p.address, d.doctor_id, d.first_name, d.last_name, d.department
),
location_analysis AS (
    SELECT 
        SUBSTR(patient_address, 1, INSTR(patient_address, ',') - 1) as patient_area,
        department,
        doctor_name,
        COUNT(DISTINCT patient_id) as patient_count,
        SUM(visit_count) as total_visits,
        ROUND(AVG(visit_count), 2) as avg_visits_per_patient
    FROM patient_travel_patterns
    WHERE doctor_rank = 1  -- Primary doctor for each patient
    GROUP BY SUBSTR(patient_address, 1, INSTR(patient_address, ',') - 1), department, doctor_name
),
coverage_gaps AS (
    SELECT 
        patient_area,
        department,
        COUNT(*) as current_providers,
        patient_count as potential_demand,
        CASE 
            WHEN patient_count > 100 AND current_providers < 3 THEN 'High Demand, Low Supply'
            WHEN patient_count > 50 AND current_providers < 2 THEN 'Moderate Gap'
            ELSE 'Adequate Coverage'
        END as coverage_status
    FROM location_analysis
    GROUP BY patient_area, department, patient_count
)
SELECT * FROM coverage_gaps
WHERE coverage_status != 'Adequate Coverage'
ORDER BY potential_demand DESC;