select  university,
		career, 
		inscription_date,
		null as first_name,
		nombre as last_name,
		sexo as gender,
		birth_date as "age",
		null as postal_code,
		"location",
		email 
		from jujuy_utn where to_date(inscription_date, 'YYYY/MM/DD') 
		between  '2020/09/01' and '2021/02/01'
		and university = 'universidad tecnológica nacional';

