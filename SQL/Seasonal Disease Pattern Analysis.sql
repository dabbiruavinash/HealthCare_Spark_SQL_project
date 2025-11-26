SELECT 
    diagnosis,
    EXTRACT(YEAR FROM visit_date) as year,
    EXTRACT(MONTH FROM visit_date) as month,
    TO_CHAR(visit_date, 'MONTH') as month_name,
    COUNT(*) as diagnosis_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY diagnosis)), 2) as seasonal_percentage FROM medical_records WHERE visit_date >= ADD_MONTHS(TRUNC(SYSDATE), -36)
  AND diagnosis IN ('Influenza', 'Pneumonia', 'Asthma', 'Allergic Rhinitis', 'Bronchitis')
  GROUP BY diagnosis, EXTRACT(YEAR FROM visit_date), EXTRACT(MONTH FROM visit_date), TO_CHAR(visit_date, 'MONTH') MODEL
    PARTITION BY (diagnosis, year)
    DIMENSION BY (month)
    MEASURES (diagnosis_count, seasonal_percentage, 0 as trend)
    RULES (
        trend[ANY] = (seasonal_percentage[CV()] - AVG(seasonal_percentage)[month BETWEEN 1 AND 12]) / NULLIF(AVG(seasonal_percentage)[month BETWEEN 1 AND 12], 0) * 100) ORDER BY diagnosis, year DESC, month;