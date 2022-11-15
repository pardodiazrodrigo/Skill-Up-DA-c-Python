SELECT 
    universidad AS university, 
    carrera AS career, 
	TO_DATE(fecha_de_inscripcion,'DD-Mon-YY') AS inscription_date,
    nombre AS full_name,
    sexo AS gender,
    TO_DATE(fecha_nacimiento,'DD-Mon-YY') AS date_of_birth, 
    NULL AS postal_code, 
    localidad AS location, 
    email AS email
FROM salvador_villa_maria
WHERE (universidad = 'UNIVERSIDAD_DEL_SALVADOR') 
    AND (TO_DATE(fecha_de_inscripcion,'DD-Mon-YY') BETWEEN '2020/09/01' AND '2021/02/01')