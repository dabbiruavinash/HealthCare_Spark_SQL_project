SELECT 
    d.department,
    TO_CHAR(a.appointment_date, 'HH24') as hour_of_day,
    TO_CHAR(a.appointment_date, 'DY') as day_of_week,
    COUNT(*) as appointment_count,
    AVG(a.duration_minutes) as avg_duration,
    COUNT(CASE WHEN a.status = 'Completed' THEN 1 END) as completed_count,
    ROUND(COUNT(CASE WHEN a.status = 'Completed' THEN 1 END) * 100.0 / COUNT(*), 2) as completion_rate
FROM appointments a
JOIN doctors d ON a.doctor_id = d.doctor_id
WHERE a.appointment_date >= ADD_MONTHS(TRUNC(SYSDATE), -6)
GROUP BY d.department, TO_CHAR(a.appointment_date, 'HH24'), TO_CHAR(a.appointment_date, 'DY')
MODEL 
    PARTITION BY (department)
    DIMENSION BY (hour_of_day, day_of_week)
    MEASURES (appointment_count, avg_duration, completion_rate)
    RULES (
        appointment_count['PEAK', ANY] = MAX(appointment_count)[ANY, ANY],
        avg_duration['PEAK', ANY] = AVG(avg_duration)[ANY, ANY]
    )
ORDER BY department, day_of_week, hour_of_day;