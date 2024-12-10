# TRABAJO PRACTICO-GRUPO-18
Este repositorio contiene el desarrollo de un sistema de recomendación de productos, implementado como solución para un producto AdTech. El sistema genera y sirve recomendaciones personalizadas para publicidades en línea, integrando tecnologías como Apache Airflow, FastAPI, PostgreSQL en AWS RDS, y AWS App Runner.

### Descripción del Proyecto
El sistema se divide en dos procesos principales:

**Generación de Recomendaciones**: Se procesan datos de logs de navegación y se computan métricas  como CTR y Top product. Los resultados se almacenan en una base de datos PostgreSQL en AWS RDS.


**Disponibilizar las Recomendaciones** : Una API dockerizada, desplegada en AWS App Runner, permite acceder a las recomendaciones en tiempo real.
El pipeline se ejecuta diariamente en una instancia de AWS EC2 mediante Apache Airflow, y los resultados se disponibilizan automáticamente.

## Estructura del Proyecto

- **`app/`**: Carpeta principal que contiene los archivos de la aplicación.
  - **`main.py`**: Archivo principal de la aplicación FastAPI, donde están definidas las rutas y lógica.
  - **`requirements.txt`**: Archivo que lista las dependencias necesarias para ejecutar la aplicación.
- **`Dockerfile`**: Archivo para construir la imagen Docker de la aplicación.
- **`Pipeline_test.py`**: Script que ejecuta pruebas de los DAGs en Airflow.
- **`run_pipeline_dag.py`**: Código para inicializar y ejecutar los DAGs de Airflow.
- **`README.md`**: Este archivo, que documenta el propósito del proyecto y su estructura.


