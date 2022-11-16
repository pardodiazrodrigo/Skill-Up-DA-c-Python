select  names, sexo as gender, direccion as address, emails, birth_dates, universities, inscription_dates, careers, locations
FROM public.lat_sociales_cine
where universities = 'UNIVERSIDAD-DEL-CINE' ;
