select universidad as university, carrera as career, fecha_de_inscripcion as inscription_date, null as first_name,
nombre as last_name, sexo as gender, fecha_nacimiento as age, null as postal_code, localidad as location,
email from salvador_villa_maria svm 
where to_date(fecha_de_inscripcion, 'DD/Mon/YY') between '01/Sep/20' and '01/Feb/21'
and universidad = 'UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA';