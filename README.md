# 🔌⚡ Energía España ETL Pipeline

Un sistema integral para extracción, transformación, almacenamiento y visualización analítica de datos de precios de la energía en España utilizando la API de ESIOS (Red Eléctrica Española).

## 📋 Descripción

Este proyecto implementa un pipeline de datos completo orientado a la toma de decisiones estratégicas en el mercado energético español, que:

1. **Extrae** datos históricos y actualizaciones diarias de precios de energía desde la API de ESIOS
2. **Transforma** los datos para facilitar análisis estratégico y detección de patrones
3. **Carga** la información en una base de datos PostgreSQL para consulta y análisis
4. **Visualiza** los datos mediante un dashboard ejecutivo interactivo con KPIs clave para comercializadoras

La arquitectura está basada en contenedores Docker, utilizando Apache Airflow para la orquestación del pipeline y Streamlit para la visualización avanzada de datos.

## 🏗️ Arquitectura

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   ESIOS API  ├────►   AIRFLOW    ├────►  POSTGRESQL  ├────►  STREAMLIT   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
      Datos           Orquestación        Almacenamiento     Análisis Ejecutivo
```

## 🛠️ Tecnologías Utilizadas

- **Docker & Docker Compose**: Contenedorización y gestión de servicios
- **Apache Airflow**: Orquestación del pipeline y programación de tareas
- **PostgreSQL**: Almacenamiento persistente de datos
- **Python**: Procesamiento y análisis de datos
- **Pandas**: Manipulación y transformación de datos
- **Streamlit**: Dashboard ejecutivo interactivo
- **Plotly**: Visualizaciones avanzadas para análisis de datos

## 🚀 Instalación y Uso

### Prerrequisitos

- Docker y Docker Compose
- Token de acceso a la API de ESIOS (https://www.esios.ree.es/)

### Instalación

1. Clonar el repositorio:
   ```bash
   git clone https://github.com/ronaldespinozaa/energia-espana-etl.git
   cd energia-espana-etl
   ```

2. Crear archivo `.env` con las credenciales necesarias:
   ```
   # PostgreSQL
   POSTGRES_USER=airflow
   POSTGRES_PASSWORD=airflow
   POSTGRES_DB=airflow
   POSTGRES_HOST=postgres
   POSTGRES_PORT=5432

   # Airflow
   AIRFLOW_ADMIN_USER=admin
   AIRFLOW_ADMIN_PASSWORD=admin
   FERNET_KEY=Em54HnzX3in-J91BUiV7awJQpPBss7UL1C5Nnv95m_k=
   AIRFLOW__WEBSERVER__SECRET_KEY=fb14d813c07bcfd892a06313fb413259b45e2ddad8a707b4071a77cb3de1961e

   # ESIOS API
   ESIOS_TOKEN=tu_token_aqui
   ```

3. Iniciar los servicios:
   ```bash
   docker-compose up -d
   ```

4. Inicializar y cargar datos históricos:
   ```bash
   docker-compose exec etl_airflow airflow dags trigger initial_load_energy_prices
   ```

### Uso

- **Airflow UI**: http://localhost:8080 (usuario: admin, contraseña: admin)
- **Dashboard Ejecutivo**: http://localhost:8501
- **PostgreSQL**: localhost:5432 (usuario: airflow, contraseña: airflow)

## 📊 Características del Dashboard Ejecutivo

El dashboard está optimizado para la toma de decisiones en comercializadoras de energía:

- **KPIs de negocio**: Indicadores clave con comparativas entre periodos
- **Análisis por franjas horarias**: Segmentación Valle, Llano y Punta para optimización de compras
- **Evaluación de riesgos**: Identificación de horas críticas con precios elevados
- **Patrones semanales**: Análisis de comportamiento por día de la semana
- **Distribución estadística**: Percentiles clave para evaluación de riesgos (P10, P50, P90)
- **Recomendaciones estratégicas**: Sugerencias automáticas basadas en datos para comercializadoras

## 📈 Pipeline de Datos

El sistema implementa dos flujos principales gestionados por Airflow:

1. **Carga inicial histórica** (`initial_load_energy_prices`):
   - Extrae datos históricos de los últimos 3 años
   - Procesa y transforma los datos para análisis
   - Carga masiva en PostgreSQL
   - Genera respaldos en CSV

2. **Actualización diaria** (`daily_update_energy_prices`):
   - Programada para ejecutarse cada mañana a las 8:00
   - Recupera datos del día anterior
   - Actualiza la base de datos manteniendo la integridad
   - Prepara datos para el dashboard

## 🗄️ Estructura del Proyecto

```
proyecto-etl/
│
├── airflow/                  # Configuración y DAGs de Airflow
│   ├── dags/                 # DAGs para el pipeline
│   │   ├── initial_load_energy_prices_dag.py  # Carga histórica
│   │   └── daily_update_energy_prices_dag.py  # Actualización diaria
│   ├── plugins/              # Plugins personalizados
│   └── logs/                 # Logs de ejecución
│
├── data/                     # Almacenamiento de datos
│   ├── raw/                  # Datos sin procesar de API
│   │   └── historical/       # Respaldos históricos
│   └── processed/            # Datos transformados
│
├── dashboard/                # Aplicación Streamlit
│   └── energy_dashboard.py   # Dashboard ejecutivo
│
├── docker/                   # Configuración de Docker
│   ├── postgres/             # Configuración PostgreSQL
│   │   └── init.sql          # Inicialización de tablas
│   ├── airflow/              # Configuración Airflow
│   └── streamlit/            # Configuración Streamlit
│
├── scripts/                  # Scripts de procesamiento
│   └── python/               # Scripts para ETL
│       └── extract_historical_data.py  # Procesamiento principal
│
└── docker-compose.yml        # Definición de servicios
```

## 🔍 Explorando los Datos

Para conectarse directamente a la base de datos:

```bash
docker exec -it etl_postgres psql -U airflow -d airflow
```


## 🛣️ Roadmap

- [x] Implementar extracción completa de datos ESIOS (histórica y diaria)
- [x] Crear pipeline ETL automatizado con Airflow
- [x] Implementar almacenamiento en PostgreSQL con detección de duplicados
- [x] Desarrollar dashboard ejecutivo para comercializadoras
- [ ] Implementar predicciones de precios usando modelos ML
- [ ] Añadir sistema de alertas por email/SMS
- [ ] Incorporar análisis de otros mercados (MIBGAS, CO2)
- [ ] Desarrollar API REST para acceso programático
- [ ] Implementar módulo de optimización de compras

## 🤝 Contribuciones

Las contribuciones son bienvenidas. Para cambios importantes, por favor abra un issue primero para discutir lo que le gustaría cambiar.

## 📄 Licencia

Este proyecto está licenciado bajo la Licencia MIT - vea el archivo [LICENSE](LICENSE) para más detalles.

## 📧 Contacto

Ronald Espinoza - [espinozajr52@gmail.com](mailto:espinozajr52@gmail.com)

Linkedin - [https://www.linkedin.com/in/ronaldespinoza/](https://www.linkedin.com/in/ronaldespinoza/)

Link del proyecto: [https://github.com/ronaldespinozaa/etl_energyprice_airflow_streamlit](https://github.com/ronaldespinozaa/etl_energyprice_airflow_streamlit)