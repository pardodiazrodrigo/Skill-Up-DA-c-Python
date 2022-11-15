select  universidad as university,
		careers as career,
		fecha_de_inscripcion as inscription_date,
		null as first_name,
		names as last_name,
		sexo as gender, 
		birth_dates as "age",
		codigo_postal as postal_code,
		null as "location",
		correos_electronicos as email 
		from palermo_tres_de_febrero
		where to_date(fecha_de_inscripcion,'DD/Mon/YY') 
		between '01/Sep/20' and '01/Feb/21'							
		and universidad = 'universidad_nacional_de_tres_de_febrero' ;