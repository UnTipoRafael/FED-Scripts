create database FED_INDICADORES
go

USE FED_INDICADORES;

drop table TRAMAJESUS
CREATE TABLE TRAMAJESUS (
    aniomes VARCHAR(6),
    id_tipo_doc VARCHAR(10),
    Id_Genero VARCHAR(10),
    num_doc VARCHAR(20),
    Id_Cita BIGINT,
    Anio INT,
    Mes INT,
    Dia INT,
    periodo VARCHAR(10),
    Lote VARCHAR(20),
    Num_Pag INT,
    Num_Reg INT,
    Id_Ups VARCHAR(20),
    renaes VARCHAR(20),
    Id_Paciente VARCHAR(20),
    Id_Personal BIGINT,
    Id_Registrador BIGINT,
    Id_Financiador VARCHAR(20),
    Id_Condicion_Establecimiento VARCHAR(20),
    Id_Condicion_Servicio VARCHAR(20),
    Edad_Reg FLOAT,
    id_tipedad_reg VARCHAR(10),
    Anio_Actual_Paciente INT,
    Mes_Actual_Paciente INT,
    Dia_Actual_Paciente INT,
    Id_Turno VARCHAR(20),
    cod_item VARCHAR(50),
    id_tipitem VARCHAR(20),
    Valor_Lab VARCHAR(50),
    Id_Correlativo_Item VARCHAR(50),
    Id_Correlativo_Lab VARCHAR(50),
    Peso FLOAT,
    Talla FLOAT,
    Hemoglobina FLOAT,
    Perimetro_Abdominal FLOAT,
    Perimetro_Cefalico FLOAT,
    Peso_Pregestacional FLOAT,
    Id_Otra_Condicion VARCHAR(20),
    Id_Centro_Poblado VARCHAR(20),
    Fecha_Ultima_Regla VARCHAR(50),
    Fecha_Solicitud_Hb VARCHAR(50),
    Fecha_Resultado_Hb VARCHAR(50),
    Fecha_Registro VARCHAR(50),
    Fecha_Modificacion VARCHAR(50),
    Id_Pais VARCHAR(10),
    Fecha_Nacimiento_Paciente VARCHAR(50)
);


select top 10 * from TRAMAJESUS

BULK INSERT TRAMAJESUS
FROM 'H:\TRAMAJESUS.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    CODEPAGE = 'ACP'
)

DBCC OPENTRAN('FED_INDICADORES');
