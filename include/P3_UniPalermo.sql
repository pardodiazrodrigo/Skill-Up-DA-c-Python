SELECT universidad AS university,
    careers AS career,
    TO_DATE(fecha_de_inscripcion, 'DD/Mon/YY') AS inscription_date,
    NULL AS first_name,
    names AS last_name,
    sexo AS gender,
    birth_dates AS age,
    codigo_postal AS postal_code,
    NULL AS location,
    correos_electronicos AS email
FROM palermo_tres_de_febrero
WHERE (
        TO_DATE(fecha_de_inscripcion, 'DD/Mon/YY') BETWEEN '01/Sep/20' AND '01/Feb/21'
    )
    AND universidad LIKE '%palermo';