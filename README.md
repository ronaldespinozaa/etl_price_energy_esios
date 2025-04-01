# 🔌⚡ Energía España ETL Pipeline

Un pipeline ETL para extraer, transformar y visualizar datos de precios de la energía en España, utilizando la API de ESIOS (Red Eléctrica Española).


## 📋 Descripción

Este proyecto implementa un pipeline de datos completo que:

1. **Extrae** datos diarios de precios de la energía desde la API de ESIOS
2. **Transforma** los datos para facilitar su análisis
3. **Carga** la información en una base de datos PostgreSQL
4. **Visualiza** los datos a través de un dashboard interactivo

La arquitectura está basada en contenedores Docker, utilizando Apache Airflow para la orquestación del pipeline y Streamlit para la visualización.

## 🏗️ Arquitectura

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   ESIOS API  ├────►   AIRFLOW    ├────►  POSTGRESQL  ├────►  STREAMLIT   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
      Datos           Orquestación        Almacenamiento      Visualización
```

## 🛠️ Tecnologías Utilizadas

- **Docker & Docker Compose**: Contenedorización
- **Apache Airflow**: Orquestación del pipeline
- **PostgreSQL**: Almacenamiento de datos
- **Python**: Procesamiento de datos
- **Pandas**: Manipulación de datos
- **Streamlit**: Visualización y dashboard
- **Plotly**: Gráficos interactivos

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

3. Iniciar los servicios:
   ```bash
   docker-compose up -d
   ```

4. Inicializar Airflow:
   ```bash
   docker-compose up airflow-init
   ```

### Uso

- **Airflow UI**: http://localhost:8080 (usuario: admin, contraseña: admin)
- **Dashboard**: http://localhost:8501
- **PostgreSQL**: localhost:5432 (usuario: airflow, contraseña: airflow)

## 📊 Características del Dashboard

- **Visualización de precios diarios**: Evolución temporal del precio del kWh
- **Análisis por hora**: Mapa de calor de precios por hora del día
- **Estadísticas**: Precios medios, máximos y mínimos por día
- **Filtros interactivos**: Selección de períodos y granularidad

## 🗄️ Estructura del Proyecto

```
proyecto-etl/
│
├── airflow/                  # Configuración y DAGs de Airflow
│   ├── dags/                 # DAGs para el pipeline
│   ├── plugins/              # Plugins personalizados
│   └── logs/                 # Logs de ejecución
│
├── data/                     # Almacenamiento de datos
│   ├── raw/                  # Datos sin procesar
│   └── processed/            # Datos procesados
│
├── dashboard/                # Aplicación Streamlit
│   ├── app.py                # Dashboard principal
│   └── utils/                # Utilerías para el dashboard
│
├── docker/                   # Configuración de Docker
│   ├── postgres/             # Configuración PostgreSQL
│   ├── airflow/              # Configuración Airflow
│   └── streamlit/            # Configuración Streamlit
│
├── scripts/                  # Scripts auxiliares
│   ├── sql/                  # Consultas SQL
│   └── python/               # Scripts Python
│
└── docker-compose.yml        # Definición de servicios
```

## 📈 Pipeline de Datos

El pipeline ETL está implementado como un DAG de Airflow y consiste en:

1. **Extracción**: Obtiene datos diarios desde la API de ESIOS
2. **Transformación**: Procesa y prepara los datos para análisis
3. **Carga**: Almacena los datos en PostgreSQL
4. **Agregación**: Calcula estadísticas diarias

## 🔍 Explorando los Datos

Para conectarse directamente a la base de datos:

```bash
docker exec -it etl_postgres psql -U airflow -d airflow
```

Consultas SQL útiles:
```sql
-- Ver precios diarios
SELECT * FROM energia.precios_diarios ORDER BY fecha DESC, hora ASC LIMIT 24;

-- Ver estadísticas
SELECT * FROM energia.estadisticas_diarias ORDER BY fecha DESC LIMIT 7;
```

## 🛣️ Roadmap

- [x] Implementar extracción básica de datos ESIOS
- [x] Crear pipeline ETL con Airflow
- [x] Implementar almacenamiento en PostgreSQL
- [x] Crear dashboard con Streamlit
- [ ] Añadir más indicadores energéticos
- [ ] Implementar predicciones de precios
- [ ] Añadir alertas basadas en umbrales de precio
- [ ] Expandir análisis histórico

## 🤝 Contribuciones

Las contribuciones son bienvenidas. Para cambios importantes, por favor abra un issue primero para discutir lo que le gustaría cambiar.

## 📄 Licencia

Este proyecto está licenciado bajo la Licencia MIT - vea el archivo [LICENSE](LICENSE) para más detalles.

## 📧 Contacto

Ronald Espinoza - [espinozajr52@gmail.com](mailto:espinozajr52@gmail.com)

Link del proyecto: [https://github.com/ronaldespinozaa/energia-espana-etl](https://github.com/ronaldespinozaa/energia-espana-etl)