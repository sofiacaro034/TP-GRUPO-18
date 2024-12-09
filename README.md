# TRABAJO PRACTICO-GRUPO-18
Este repositorio contiene el desarrollo de un sistema de recomendación de productos, implementado como solución para un producto AdTech. El sistema genera y sirve recomendaciones personalizadas para publicidades en línea, integrando tecnologías como Apache Airflow, FastAPI, PostgreSQL en AWS RDS, y AWS App Runner.

### Descripción del Proyecto
El sistema se divide en dos procesos principales:
**Generación de Recomendaciones**: Se procesan datos de logs de navegación y se computan métricas  como CTR y Top product. Los resultados se almacenan en una base de datos PostgreSQL en AWS RDS.
**Servir las Recomendaciones** : Una API dockerizada, desplegada en AWS App Runner, permite acceder a las recomendaciones en tiempo real.
El pipeline se ejecuta diariamente en una instancia de AWS EC2 mediante Apache Airflow, y los resultados se disponibilizan automáticamente.

### Requisitos
    Dependencias Principales
Python: 3.8 
Apache Airflow: 2.8.4
FastAPI
PostgreSQL en AWS RDS
Docker: Para la API
Librerías de Python:pandas,psycopg2,logging

