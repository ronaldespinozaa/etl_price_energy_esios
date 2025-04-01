-- Cargar datos desde el CSV procesado a la tabla de precios
INSERT INTO energia.precios_diarios (fecha, hora, precio_kwh, unidad)
SELECT 
    TO_DATE(fecha, 'YYYY-MM-DD'),
    hora::INTEGER,
    precio_kwh::NUMERIC(10,5),
    unidad
FROM 
    temp_prices
ON CONFLICT (fecha, hora) 
DO UPDATE SET 
    precio_kwh = EXCLUDED.precio_kwh,
    unidad = EXCLUDED.unidad;

-- Actualizar estadísticas diarias
INSERT INTO energia.estadisticas_diarias 
(fecha, precio_medio, precio_maximo, precio_minimo, hora_pico, hora_valle)
SELECT 
    fecha,
    AVG(precio_kwh) as precio_medio,
    MAX(precio_kwh) as precio_maximo,
    MIN(precio_kwh) as precio_minimo,
    (SELECT hora FROM energia.precios_diarios pd 
     WHERE pd.fecha = p.fecha 
     ORDER BY precio_kwh DESC LIMIT 1) as hora_pico,
    (SELECT hora FROM energia.precios_diarios pd 
     WHERE pd.fecha = p.fecha 
     ORDER BY precio_kwh ASC LIMIT 1) as hora_valle
FROM 
    energia.precios_diarios p
WHERE 
    fecha = '{{ ds }}'
GROUP BY 
    fecha
ON CONFLICT (fecha) 
DO UPDATE SET 
    precio_medio = EXCLUDED.precio_medio,
    precio_maximo = EXCLUDED.precio_maximo,
    precio_minimo = EXCLUDED.precio_minimo,
    hora_pico = EXCLUDED.hora_pico,
    hora_valle = EXCLUDED.hora_valle;