# Ingestión de API Rick and Morti

Con estos pasos podrán ejecutar el docker-compose para ejecutar diariamente en un cluster de Airflow datos de los personajes de esta serie.

## Requisitos

- Requisitos de Hardware:

    La cantidad de recursos (CPU, RAM, almacenamiento) necesarios dependerá de la carga de trabajo y la cantidad de DAGs y tareas que planificas ejecutar.
    Se recomienda al menos 2 CPU y 4 GB de RAM como punto de partida para entornos de desarrollo y pruebas. Los entornos de producción pueden requerir más recursos.
- Sistema Operativo:

    Linux (Ubuntu, CentOS, Debian) es la opción más común y recomendada.
    Windows también es compatible, pero puede requerir configuraciones adicionales y puede haber limitaciones en ciertas características.
- Python:

    Airflow requiere Python 3.6 o posterior. Se recomienda utilizar una versión de Python 3.7 o superior para aprovechar todas las características y mejoras.

## Instalación y Configuración

1. Clona este repositorio: https://github.com/rololevy/DE_Coder_House/tree/05c0f56665d99fc0ddccaf786f211a7a5a72018f/Entrega_3
2. Instala las dependencias: `pip install -r requirements.txt`
3. Accede al servidor web de Airflow en el que deseas importar las variables y sigue estos pasos:
    Ve a la pestaña "Admin" en la barra de navegación superior y selecciona "Variables".
    Busca la opción "Import" o "Import Variables". Esto también puede variar según la versión de Airflow.
    Selecciona el archivo JSON que exportaste anteriormente desde la otra instancia de Airflow.
    Al importar el archivo JSON, las variables se agregarán a la lista de variables de esta instancia.

## Uso

1. Una vez creada descardo el repositorio en tu directorio, podrías utilizar un entorno virtual.
2. Luego ejecuta docker-compose init airflow
3. Y por último docker-compose up
4. No te olvides de setear las variables en el web server de airflow con el archivo config.json proporcionado en formato zip(con el pwd provista), este tiene las credenciales para conectarse a Redshift.

## Estructura del Proyecto

- El repo contiene 4 archivos: el docker-compose, en el directorio principal, el .py del dag y del script que realiza el etl, y el config.json que contiene las credenciales. Adicionalmente se crean los directorios de práctica.

## Autor

- Orlando Aguilera.
- Linkedin: https://www.linkedin.com/in/orlando-aguilera-analista/


