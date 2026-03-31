## Prueba Técnica - Data Engineer | Procesamiento de Transacciones Fintech

## Descripción General

Este proyecto implementa un pipeline de datos end-to-end para el procesamiento de transacciones financieras, simulando un sistema real de pagos en una fintech.

El pipeline permite:

-Ingestar datos crudos en formato CSV

-Limpiar y validar la información

-Detectar transacciones sospechosas (fraude)

-Almacenar los datos en un Data Warehouse para análisis

## Arquitectura

-Data Lake: Carpeta local ./transactions donde se almacenan archivos CSV crudos

-ETL: Script en Python que procesa los datos cada minuto

-Processed: Carpeta ./processed con datos limpios

-Suspicious: Carpeta ./suspicious con transacciones sospechosas

-Data Warehouse: PostgreSQL con modelo dimensional

-Imagen en la raiz del proyecto:arquitectura.png

## Instalación
1. Clonar repositorio
git clone <tu-repositorio>
cd <repo>
2. Crear entorno virtual
python3 -m venv venv
source venv/bin/activate
3. Instalar dependencias
pip install -r requirements.txt

## Configuración de Base de Datos

CREATE DATABASE fintech;

Crear las tablas:

CREATE TABLE dim_users (
    user_id VARCHAR PRIMARY KEY,
    country VARCHAR
);

CREATE TABLE dim_merchants (
    merchant_id VARCHAR PRIMARY KEY
);

CREATE TABLE fact_transactions (
    transaction_id VARCHAR PRIMARY KEY,
    user_id VARCHAR,
    amount NUMERIC,
    timestamp TIMESTAMP,
    status VARCHAR,
    country VARCHAR
);

## Ejecución

1. Ejecutar pipeline ETL (simulación en tiempo real)
python main.py

Esto:

-Genera transacciones cada minuto

-Limpia y valida datos

-Detecta transacciones sospechosas

-Guarda resultados en processed/ y suspicious/

## Cargar datos al Data Warehouse

python3 -m db.load_data

NOTA: Tener presente la configuración de la base de datos en el archivo db/connection.py

## Estrategia de Limpieza de Datos

La función clean_data() realiza:

-Eliminación de duplicados

-Manejo de valores nulos

-Validación de tipos de datos

-Eliminación de outliers (montos > 10,000)

-Estandarización de campos categóricos (country, currency, status)

## Estrategia de Detección de Fraude

La función detect_suspicious_transactions() implementa reglas heurísticas:

-Montos altos (> 1000)

-Múltiples intentos fallidos por usuario

-Transacciones con estado declined

-Transacciones internacionales

-Alta frecuencia de transacciones por usuario

-Países de alto riesgo (ej: NG, RU)

Salida:

Transacciones normales

Transacciones sospechosas

## Modelo de Datos
Se implementó un modelo dimensional tipo Star Schema:

Tabla de Hechos:
fact_transactions

Tablas de Dimensión:
dim_users
dim_merchants

## Limitaciones
-Detección de fraude basada en reglas (no machine learning)

-No se implementó orquestación (Airflow)

-No se implementó streaming en tiempo real (Kafka)

-Modelo dimensional parcial (faltan dim_time, dim_payment_methods)

##  Mejoras Futuras
-Implementar Apache Kafka para procesamiento en tiempo real

-Orquestar el pipeline con Apache Airflow

-Extender el modelo dimensional (dim_time, dim_payment_methods)

-Integrar API para conversión de monedas

-Agregar tests y validaciones automáticas

## Tiempo Invertido

Aproximadamente 8 horas, enfocadas en:

-Diseño del pipeline

-Calidad de datos

-Modelado y carga al Data Warehouse

## Notas Finales

Esta solución prioriza:

Simplicidad
Correctitud
Reproducibilidad

Aunque no todas las fases fueron implementadas completamente, el pipeline desarrollado demuestra competencias sólidas en ingeniería de datos, incluyendo ETL, modelado dimensional y validación de datos.

Autor: [Lina Marcela Colorado Vivas]
Fecha: 2026

