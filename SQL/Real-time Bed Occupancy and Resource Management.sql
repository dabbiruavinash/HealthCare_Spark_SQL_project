WITH current_appointments AS (
    SELECT 
        a.room_number,
        d.department,
        COUNT(*) as current_patients,
        AVG(a.duration_minutes) as avg_stay_duration
    FROM appointments a
    JOIN doctors d ON a.doctor_id = d.doctor_id
    WHERE a.appointment_date >= TRUNC(SYSDATE)
      AND a.appointment_date < TRUNC(SYSDATE) + 1
      AND a.status IN ('Scheduled', 'In Progress')
    GROUP BY a.room_number, d.department
),
department_capacity AS (
    SELECT 
        department,
        COUNT(DISTINCT room_number) as total_rooms,
        SUM(current_patients) as occupied_rooms,
        ROUND(SUM(current_patients) * 100.0 / COUNT(DISTINCT room_number), 2) as occupancy_rate
    FROM current_appointments
    GROUP BY department
),
predicted_demand AS (
    SELECT 
        dc.department,
        dc.total_rooms,
        dc.occupied_rooms,
        dc.occupancy_rate,
        AVG(ca.avg_stay_duration) as avg_processing_time,
        CASE 
            WHEN dc.occupancy_rate > 80 THEN 'High Demand - Add Resources'
            WHEN dc.occupancy_rate > 60 THEN 'Moderate Demand - Monitor'
            ELSE 'Low Demand - Optimal'
        END as resource_status
    FROM department_capacity dc
    JOIN current_appointments ca ON dc.department = ca.department
    GROUP BY dc.department, dc.total_rooms, dc.occupied_rooms, dc.occupancy_rate
)
SELECT * FROM predicted_demand
ORDER BY occupancy_rate DESC;