# weather-data-project
Proyecto pipeline de datos climáticos diarios e históricos.

## Descripción General Del Proyecto.

La idea del proyecto es obtener diariamente los datos del clima en distintas localidades Españolas y dejar un registro histórico de los datos recaudados.

Voy a orquestar una pipeline en la que obtendré los datos, los procesaré y transformaré para obtener los resultados deseados, la finalidad será generar un registro de datos adecuado para una capa de reporting.

En principio las teconlogías involucradas en el proyecto será la API de Open Meteo, Airflow como herramienta de orquestación, Databricks como herramienta de procesamiento de datos, Azure DataLake como capa de almacenamiento y como lenguaje para el desarrollo usaré Python y SQL.

## Arquitectura Proyecto.
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
  
## Funcionamiento del proyecto.
  
- Desplegar contendores Docker :
  <img width="1569" height="204" alt="Captura desde 2025-12-14 19-21-17" src="https://github.com/user-attachments/assets/57ba5d05-3053-4cf6-880b-3c07d05486ac" />
- La imagen de Airflow instalada en el contenedor diariamente ejecuta la extración y subida a langing de los datos:
  <img width="2551" height="494" alt="Captura desde 2025-12-14 19-24-25" src="https://github.com/user-attachments/assets/88aabc53-2eae-491c-a5b3-abfd533d1169" />
- Datos llegan a landing:
  <img width="2551" height="494" alt="Captura desde 2025-12-14 19-25-20" src="https://github.com/user-attachments/assets/ba8363cd-0751-4db6-9e9e-dac61729e523" />
- Son procesados por job de Databicks encargados del paso de langing a staging y staging a common:
  <img width="2551" height="514" alt="Captura desde 2025-12-14 19-26-59" src="https://github.com/user-attachments/assets/9b0e341d-073f-4142-a4cb-45a3e322aa9f" />
  (En este paso los datos son enriquecidos, se obtienen país y ciudad a partir de la latitud y longitud)
- Los datos llegan a common, particionados por fecha, país, ciudad y hora, permitiendo llevar un registro diario del clíma:
  <img width="2551" height="514" alt="image" src="https://github.com/user-attachments/assets/defe2628-787a-4360-a139-c94bd230a815" />

## Puntos de mejora:
- Registrar los datos clímaticos de varias ubicaciones en la pipeline, esto no me ha sido posible por limitaciones de la versión gratuita de API.
- Dag de borrado de archivos de backup que quedan en los contenedores, me refiero a los archivos que quedan en shared, si fuese a mantener el proyecto activo semanalmente los borraría, por el costo de mantener Azure activo no lo mantendré, así que no es necesario.
  
