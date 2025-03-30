Los archivos obtenidos del procesamiento en [Transform Main script](./TablesTransformMain.py) se guardan en archivos CSV dentro de la VM. Luego se accede a estos archivos con ayuda del script [LoadFilesBQ](./LoadFilesBQ.py) para poderlos subir a nuestro respositorio en la nube. 

Este script basicamente consta de tres secciones:

- Carga de credenciales para acceso al repositorio (que previamente debio configurarse en la terminal de google cloud computing)
- Carga de archivos CSV para su formateo especifico de tipos de datos, con los que se cargaran al repositorio
- Creacion de configuaraciones para el formateo de tipos de datos para cada tabla
- Carga de las tablas con la configuracion establecida, a nuestro repositorio.

Al revisar el script podra notarse que se eliminan las tablas cargadas anteriormente (carga del dia anterior). Dado que la carga y almacenamiento de informacion de BQ es de muy bajo costo, heos decidido hacerlo asi para poder coregir posible errores en cargas anteiores dentro de los historicos, ademas para poder hacer un hard reset en caso de algun mal funcionamiento de nuestro proceso y poder eliminar todo y cargarlo nuevamente. Sin embargo, teoricamente, este no es lo mas viable. El ideal seria solo añadir los registros nuevos a las tablas ya existente en el repositorio, y tener la posibilidad de hacer un hard reset de nuestras tablas en el repositorio con ayuda de otro modulo. Esta es una funcionalidad que aun se sigue trabajando y se actualizara el script una vez que se consiga. 