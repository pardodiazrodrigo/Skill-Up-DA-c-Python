select universidad as university, carrera as career, fecha_de_inscripcion as inscription_date,
null as first_name, name as last_name, sexo as gender, fecha_nacimiento as age, codigo_postal as postal_code, null as location,
correo_electronico as email from flores_comahue
where to_date(fecha_de_inscripcion, 'YYYY/MM/DD') between '2020/09/01' and '2021/02/01'
and universidad = 'UNIVERSIDAD DE FLORES';
