SELECT 
    universidad AS university, 
    carrera AS career, 
	TO_DATE(fecha_de_inscripcion,'YYYY-MM-DD') AS inscription_date,
    name AS full_name,
    sexo AS gender, 
    TO_DATE(fecha_nacimiento,'YYYY-MM-DD') AS date_of_birth, 
    codigo_postal AS postal_code, 
    NULL AS location, 
    correo_electronico AS email
FROM flores_comahue
WHERE (universidad = 'UNIV. NACIONAL DEL COMAHUE') 
    AND (TO_DATE(fecha_de_inscripcion,'YYYY-MM-DD') BETWEEN '2020/09/01' AND '2021/02/01')