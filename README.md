# Skill-Up-DA-c-Python
user: mollymay999
nombre: Maria del Mar Contaldi
mail: mmarcontaldi@yahoo.com.ar 
Grupo: 3

Como parte del proyecto del skill up de alkemy, en este directorio-branch
se presentan algunos resultados logrados siguiendo el backlog ofrecido, para el grupo de universidades H.

Para operar en airflow he accedido a la prueba gratuita de google cloud donde el cloud composer me
ha permitido ver los dags producidos y su funcionalidad. Debo  decir que en casi 10 días he consumido
gran parte de mis créditos (cerca de 200USD) pero aún así me ha facilitado el proceso al trabajar en la nube.

Los operadores utilizados con éxito son el BashOperator y el PythonOperator, 
con ellos se generan las tareas para darles luego dependencias.
Se creó para ello un bucket en google cloud y otro en AWS S3 como solicitaba el ejercicio.
Aun así, aún no  logré la conexión entre cloud composer y las bases de datos remotas, probablemente por error
de ingreso de datos de conexión. 

Para este proyecto se trabajó en generar la extracción, transformación y carga de datos en python y en 
formato de dag, de forma secuencial desde generar la estructura, y luego lograr la carga y gestión de datos.
En última instancia se armaron dags dinámicos, sobre los cuales me encuentro aún trabajando, pero que los 
de mi grupo han conseguido desarrollar con éxito. Ademmás se armaron python notebooks para la producción,
la interpretacón de los datos y su visualización.

Como  recursos fueron de gran ayuda la documentación oficial de python, apache airflow, google cloud, astronomer y aws.
Los tutoriales en youtube, y blogs de real python, geeksforgeeks, stackoverflow, stackabuse y w3, entre otros, 
fueron de gran utilidad.
