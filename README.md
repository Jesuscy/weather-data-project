# weather-data-project
Proyecto pipeline de datos climáticos diarios e históricos.

03/11/2025

## Descripción General Del Proyecto.

La idea del proyecto es obtener diariamente los datos del clima en distintas localidades Españolas y dejar un registro histórico de los datos recaudados.

Voy a orquestar una pipeline en la que obtendré los datos, los procesaré y transformaré para obtener los resultados deseados, la finalidad será generar un registro de datos adecuado para una capa de reporting.

En principio las teconlogías involucradas en el proyecto será la API de Open Meteo, Airflow como herramienta de orquestación, Databricks como herramienta de procesamiento de datos, Azure DataLake como capa de almacenamiento y como lenguaje para el desarrollo usaré Python y SQL.

## Tecnologías usadas.
En este proyecto he empleado las siguientes tecnologías:
- Como orquestador utilizo un servidor de Airflow que corre en un Contenedor Docker, he trabajado anteriormente con soluciones como Astro CLI para simplificar el despliegue, pero con la finalidad de tener más control sobre el entorno y asegurar su continuidad esta vez    elijo docker.
- Para el almacenamiento de datos empleo un Azure Datalake Gen 2, por la cercanía que tengo a Azure he elegido esta opción.
- Para el procesamiento de los datos he decidido usar Databricks, configurando jobs desencadenados por la ingesta de datos.
- Los scripts de extracción de datos han sido realizados en Python, y la transformación de datos en PySpark.
 
