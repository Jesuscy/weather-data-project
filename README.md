# weather-data-project
Proyecto pipeline de datos climáticos diarios e históricos.

## Descripción General Del Proyecto.

La idea del proyecto es obtener diariamente los datos del clima en distintas localidades Españolas y dejar un registro histórico de los datos recaudados.

Voy a orquestar una pipeline en la que obtendré los datos, los procesaré y transformaré para obtener los resultados deseados, la finalidad será generar un registro de datos adecuado para una capa de reporting.

En principio las teconlogías involucradas en el proyecto será la API de Open Meteo, Airflow como herramienta de orquestación, Databricks como herramienta de procesamiento de datos, Azure DataLake como capa de almacenamiento y como lenguaje para el desarrollo usaré Python y SQL.

# Arquitectura Proyecto.
<img width="1220" height="747" alt="pipeline_arq_schema" src="https://github.com/user-attachments/assets/c5060df4-3d5e-460c-b100-1557d58d2628" />

## Tecnologías Usadas.
En este proyecto he empleado las siguientes tecnologías:
- Como orquestador utilizo un servidor de Airflow que corre en un Contenedor Docker, he trabajado anteriormente con soluciones como Astro CLI para simplificar el despliegue, pero con la finalidad de tener más control sobre el entorno y asegurar su continuidad esta vez    elijo docker.
- Para el almacenamiento de datos empleo un Azure Datalake Gen 2, por la cercanía que tengo a Azure he elegido esta opción.
- Para el procesamiento de los datos he decidido usar Databricks, configurando jobs desencadenados por la ingesta de datos.
- Los scripts de extracción de datos han sido realizados en Python, y la transformación de datos en PySpark.
 
## Cuaderno de Desarrollo.
Aquí iré añadiendo todas las tareas que he tenido que llevar a cabo para dejar el proyecto funcional.
- Instalación de docker.
- Creación de imagen personalizada a partir de la imagen que ofrece Airflow en su Documentación.
- Realización de los Scripts de petición y subida a Azure, junto con los test.
- Gestión de dependencias entre la los scripts y airflow, ajustando las versiones para asegurar compatibilidad (Ha sido un paso complicado.)
- Creación de Azure Databricks Workspace y configuración de cluster.
- Gestión de acceso de Airflow y Databricks a componentes de Azure, Service Principal y demás ...
- Desarrollo de script PySpark en nootebok de Databricks.
- Creación de Jobs y Worflows para el procesamiento de los datos.
  
  
