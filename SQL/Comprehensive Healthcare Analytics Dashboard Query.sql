WITH kpi_data AS (
    -- Patient Statistics
    SELECT 'Total Patients' as metric_name, COUNT(*) as metric_value FROM patients
    UNION ALL
    SELECT 'New Patients This Month', COUNT(*) FROM patients WHERE created_date >= TRUNC(SYSDATE, 'MM')
    UNION ALL
    
    -- Appointment Statistics
    SELECT 'Total Appointments', COUNT(*) FROM appointments WHERE appointment_date >= TRUNC(SYSDATE, 'MM')
    UNION ALL
    SELECT 'Appointment Completion Rate', 
           ROUND(COUNT(CASE WHEN status = 'Completed' THEN 1 END) * 100.0 / COUNT(*), 2)
    FROM appointments WHERE appointment_date >= TRUNC(SYSDATE, 'MM')
    UNION ALL
    
    -- Financial Statistics
    SELECT 'Monthly Revenue', SUM(total_amount) FROM billing WHERE bill_date >= TRUNC(SYSDATE, 'MM')
    UNION ALL
    SELECT 'Outstanding Balance', SUM(patient_balance) FROM billing WHERE status = 'Pending'
    UNION ALL
    
    -- Clinical Statistics
    SELECT 'Average Treatment Cost', AVG(total_amount) FROM billing WHERE bill_date >= ADD_MONTHS(TRUNC(SYSDATE), -6)
    UNION ALL
    SELECT 'Readmission Rate', 
           ROUND(COUNT(CASE WHEN follow_up_required = 'Y' THEN 1 END) * 100.0 / COUNT(*), 2)
    FROM medical_records WHERE visit_date >= ADD_MONTHS(TRUNC(SYSDATE), -3)
    UNION ALL
    
    -- Department Statistics
    SELECT 'Most Active Department', 
           (SELECT department FROM doctors GROUP BY department ORDER BY COUNT(*) DESC FETCH FIRST 1 ROWS ONLY)
    FROM dual
    UNION ALL
    SELECT 'Department with Highest Revenue',
           (SELECT department FROM doctors d 
            JOIN appointments a ON d.doctor_id = a.doctor_id 
            JOIN billing b ON a.appointment_id = b.appointment_id
            WHERE b.bill_date >= TRUNC(SYSDATE, 'MM')
            GROUP BY department ORDER BY SUM(b.total_amount) DESC FETCH FIRST 1 ROWS ONLY)
    FROM dual
),
trend_analysis AS (
    SELECT 
        metric_name,
        metric_value,
        CASE 
            WHEN metric_name LIKE '%Rate%' AND metric_value > 90 THEN 'Excellent'
            WHEN metric_name LIKE '%Rate%' AND metric_value > 75 THEN 'Good'
            WHEN metric_name LIKE '%Rate%' AND metric_value > 60 THEN 'Fair'
            WHEN metric_name LIKE '%Rate%' THEN 'Needs Improvement'
            WHEN metric_name LIKE '%Revenue%' AND metric_value > 100000 THEN 'High'
            WHEN metric_name LIKE '%Revenue%' AND metric_value > 50000 THEN 'Medium'
            WHEN metric_name LIKE '%Revenue%' THEN 'Low'
            ELSE 'N/A'
        END as performance_indicator
    FROM kpi_data
)
SELECT 
    metric_name,
    metric_value,
    performance_indicator,
    CASE 
        WHEN performance_indicator IN ('Excellent', 'High') THEN '✅'
        WHEN performance_indicator IN ('Good', 'Medium') THEN '⚠️'
        WHEN performance_indicator IN ('Fair', 'Low', 'Needs Improvement') THEN '❌'
        ELSE '📊'
    END as status_icon
FROM trend_analysis
ORDER BY 
    CASE 
        WHEN metric_name LIKE 'Total%' THEN 1
        WHEN metric_name LIKE 'New%' THEN 2
        WHEN metric_name LIKE '%Rate%' THEN 3
        WHEN metric_name LIKE '%Revenue%' THEN 4
        WHEN metric_name LIKE '%Balance%' THEN 5
        WHEN metric_name LIKE '%Cost%' THEN 6
        ELSE 7
    END;