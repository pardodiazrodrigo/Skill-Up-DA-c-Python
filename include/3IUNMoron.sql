SELECT universidad AS university,
    carrerra AS career,
    TO_DATE(fechaiscripccion, 'DD/MM/YYYY') AS inscription_date,
    NULL AS first_name,
    NULL AS last_name,
    nombrre AS names,
    sexo AS gender,
    nacimiento AS birth_date,
    NULL AS age,
    codgoposstal AS postal_code,
    NULL AS location,
    eemail AS email
FROM moron_nacional_pampa
WHERE (
        TO_DATE(fechaiscripccion, 'DD/MM/YYYY') BETWEEN '01/Sep/20' AND '01/Feb/21'
    )
    AND universidad LIKE '%morón';