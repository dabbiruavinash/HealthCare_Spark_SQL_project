WITH equipment_usage AS (
    SELECT 
        a.room_number,
        d.department,
        TO_CHAR(a.appointment_date, 'YYYY-MM') as usage_month,
        COUNT(*) as appointment_count,
        SUM(a.duration_minutes) as total_usage_minutes,
        AVG(a.duration_minutes) as avg_usage_per_appointment
    FROM appointments a
    JOIN doctors d ON a.doctor_id = d.doctor_id
    WHERE a.appointment_date >= ADD_MONTHS(TRUNC(SYSDATE), -18)
    GROUP BY a.room_number, d.department, TO_CHAR(a.appointment_date, 'YYYY-MM')
),
usage_trends AS (
    SELECT 
        room_number,
        department,
        usage_month,
        appointment_count,
        total_usage_minutes,
        avg_usage_per_appointment,
        LAG(total_usage_minutes) OVER (PARTITION BY room_number ORDER BY usage_month) as prev_month_usage,
        AVG(total_usage_minutes) OVER (PARTITION BY room_number ORDER BY usage_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg_usage
    FROM equipment_usage
),
maintenance_predictions AS (
    SELECT 
        room_number,
        department,
        usage_month,
        appointment_count,
        total_usage_minutes,
        ROUND((total_usage_minutes - moving_avg_usage) / NULLIF(moving_avg_usage, 0) * 100, 2) as usage_change_percent,
        CASE 
            WHEN total_usage_minutes > 10000 THEN 'High Usage - Schedule Maintenance'
            WHEN total_usage_minutes > 8000 AND usage_change_percent > 20 THEN 'Increasing Usage - Monitor Closely'
            WHEN total_usage_minutes < 2000 THEN 'Low Usage - Consider Reallocation'
            ELSE 'Normal Operation'
        END as maintenance_status
    FROM usage_trends
    WHERE moving_avg_usage IS NOT NULL
)
SELECT * FROM maintenance_predictions
WHERE maintenance_status != 'Normal Operation'
ORDER BY total_usage_minutes DESC;