-- Inicialización de la base de datos para precios de energía
CREATE SCHEMA IF NOT EXISTS energia;

-- Tabla para almacenar precios de la energía
CREATE TABLE IF NOT EXISTS energia.precios_diarios (
    id SERIAL PRIMARY KEY,
    fecha DATE NOT NULL,
    hora INTEGER NOT NULL CHECK (hora >= 0 AND hora <= 23),
    precio_kwh NUMERIC(10, 5) NOT NULL,
    unidad VARCHAR(10) NOT NULL,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(fecha, hora)
);

-- Tabla para almacenar estadísticas agregadas
CREATE TABLE IF NOT EXISTS energia.estadisticas_diarias (
    id SERIAL PRIMARY KEY,
    fecha DATE UNIQUE NOT NULL,
    precio_medio NUMERIC(10, 5) NOT NULL,
    precio_maximo NUMERIC(10, 5) NOT NULL,
    precio_minimo NUMERIC(10, 5) NOT NULL,
    hora_pico INTEGER NOT NULL,
    hora_valle INTEGER NOT NULL,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Permisos
GRANT ALL PRIVILEGES ON SCHEMA energia TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA energia TO airflow;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA energia TO airflow;