-- Cambia a la base de datos donde quieres importar los datos
CREATE DATABASE FED_INDICADORES
USE FED_INDICADORES;

GO
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
DROP TABLE MAESTRO_PACIENTE;
-- 1. Crear la tabla donde se importarán los datos
CREATE TABLE MAESTRO_PACIENTE (
    id_paciente varchar(20),
    id_tipo_documento bigint,
    numero_documento varchar(50),
    apellido_paterno_paciente varchar(100),
    apellido_materno_paciente varchar(100),
    nombres_paciente varchar(150),
    fecha_nacimiento date,
    genero char(1),
    id_etnia bigint,
    historia_clinica varchar(50),
    ficha_familiar varchar(50),
    ubigeo_nacimiento varchar(6),
    ubigeo_reniec varchar(6),
    domicilio_reniec varchar(255),
    ubigeo_declarado varchar(6),
    domicilio_declarado varchar(255),
    referencia_domicilio varchar(255),
    id_pais varchar(3),
    id_establecimiento bigint,
    fecha_alta date,
    fecha_modificacion datetime
);
go

-- 2. Importar los datos desde el archivo CSV
BULK INSERT MAESTRO_PACIENTE
FROM 'D:\001 DIRESA 0001\MAESTRO_PACIENTE.CSV'
WITH (
    FIRSTROW = 2, -- omite la cabecera
    FIELDTERMINATOR = ',', -- separador de columnas
    ROWTERMINATOR = '\n',  -- separador de filas
    TABLOCK,
    CODEPAGE = 'ACP' -- Usa '65001' si es UTF-8
);
GO
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
------------------------------------------------------------------------------------
use FED_INDICADORES
select top 100 *  from MAESTRO_PACIENTE --where Ubigeo_Declarado is  null
------------------------------------------------------------------------------------
use FED_INDICADORES
-- 1. Crear la tabla MAESTRO_PERSONAL
drop table MAESTRO_PERSONAL;
CREATE TABLE MAESTRO_PERSONAL (
    id_personal bigint,
    id_tipo_documento bigint,
    numero_documento varchar(10),
    apellido_paterno_personal varchar(100),
    apellido_materno_personal varchar(100),
    nombres_personal varchar(150),
    fecha_nacimiento varchar(100),
    id_condicion bigint,
    id_profesion bigint,
    id_colegio bigint,
    numero_colegiatura varchar(100),
    id_establecimiento varchar(100),
    fecha_alta datetime ,
    fecha_baja datetime
);
GO

-- 2. Importar el archivo CSV (ajusta la ruta del archivo según corresponda)
BULK INSERT MAESTRO_PERSONAL
FROM 'D:\001 DIRESA 0001\MAESTRO_PERSONAL.CSV'
WITH (
    CODEPAGE = 'ACP'  ,         -- para archivos UTF-8;65001 usa 'ACP' si es ANSI
    FIRSTROW = 2,                -- omitir la cabecera
    FIELDTERMINATOR = ',',       -- separador de campos
    ROWTERMINATOR = '\n'        -- separador de registros (usar '\r\n' si es necesario)
  --  TABLOCK,
);
GO
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------

-- Crear la tabla MAESTRO_REGISTRADOR
DROP TABLE MAESTRO_REGISTRADOR
CREATE TABLE MAESTRO_REGISTRADOR (
    id_registrador bigint,
    id_tipo_documento bigint,
    numero_documento varchar(20),
    apellido_paterno_registrador varchar(100),
    apellido_materno_registrador varchar(100),
    nombres_registrador varchar(150),
    fecha_nacimiento varchar(50)
);

BULK INSERT MAESTRO_REGISTRADOR
FROM 'D:\001 DIRESA 0001\MAESTRO_REGISTRADOR.CSV'
WITH (
    FIRSTROW = 2,                       -- Salta el encabezado
    FIELDTERMINATOR = ',',             -- Separador de campos
    ROWTERMINATOR = '\n',              -- Fin de línea
    CODEPAGE = 'ACP'                -- ANSI 
   -- ERRORFILE = 'C:\Datos\errores_registrador.log'  -- Archivo log de errores (opcional pero útil)
)

SELECT * FROM MAESTRO_REGISTRADOR WHERE LEN(Apellido_Materno_Registrador )>20
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
use FED_INDICADORES
drop table NOMINAL_TRAMA_NUEVO_GENERAL_2025_6
CREATE TABLE NOMINAL_TRAMA_NUEVO_GENERAL_2025_6 (
    id_cita bigint,
    anio int,
    mes int,
    dia int,
    fecha_atencion varchar(50),
    lote varchar(20),
    mum_pag int,
    num_reg int,
    id_ups varchar(20),
    id_establecimiento varchar(20),
    id_paciente varchar(20),
    id_personal bigint,
    id_registrador bigint,
    id_financiador varchar(20),
    id_condicion_establecimiento varchar(20),
    id_condicion_servicio varchar(20),
    edad_reg float,
    tipo_edad varchar(10),
    anio_actual_paciente int,
    mes_actual_paciente int,
    dia_actual_paciente int,
    id_turno varchar(20),
    codigo_item varchar(50),
    tipo_diagnostico varchar(10),
    valor_lab varchar(50),
    id_correlativo varchar(50),
    id_correlativo_lab varchar(50),
    peso float,
    talla float,
    hemoglobina float,
    perimetro_abdominal float,
    perimetro_cefalico float,
    id_otra_condicion varchar(20),
    id_centro_coblado varchar(20),
    fecha_ultima_regla varchar(50),
    fecha_solicitud_hb varchar(50),
    fecha_resultado_hb varchar(50),
    fecha_registro varchar(50),
    fecha_modificacion varchar(50),
    id_pais varchar(20),
    gruporiesgo_desc varchar(100),
    condicion_gestante varchar(20),
    peso_pregestacional float,
    id_dosis varchar(20),
    renipress varchar(50),
    id_institucion_edu varchar(20),
    id_aplicacionorigen varchar(20),
    alerta nvarchar(max)
)



BULK INSERT NOMINAL_TRAMA_NUEVO_GENERAL_2025_6
FROM 'D:\001 DIRESA 0001\NOMINAL_TRAMA_NUEVO_GENERAL_2025_6.CSV'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    CODEPAGE = 'ACP'
);


select top 100 * from NOMINAL_TRAMA_NUEVO_GENERAL_2025_6 

*-

----------------------------------------

drop table Renaes
--Renaes_20250320.csv
CREATE TABLE Renaes (
    cod_estab varchar(20),
    ubigeo varchar(10),
    ubi_prov varchar(10),
    sector varchar(50),
    clasifica varchar(600),
    tipo varchar(50),
    direcc varchar(300),
    fono varchar(200),
    tipocatego varchar(50),
    nrocategor varchar(50),
    horario varchar(100),
    iniciodeac varchar(50),
    director varchar(150),
    ruc varchar(50),
    cod_ue varchar(50),
    unid_ejec varchar(100),
    sw_estado varchar(50),
    inst varchar(50),
    norte varchar(50),
    este varchar(50),
    cota varchar(20),
    cat_estab varchar(50),
    clas_estab varchar(50),
    tip_estab varchar(50),
    desc_estab varchar(255),
    desc_dpto varchar(100),
    desc_prov varchar(100),
    desc_dist varchar(100),
    cod_dpto varchar(100),
    cod_prov varchar(10),
    cod_dist varchar(10),
    cod_disa varchar(10),
    cod_red varchar(10),
    cod_mred varchar(10),
    desc_disa varchar(100),
    desc_red varchar(100),
    desc_mred varchar(100),
    ambito varchar(50),
    disa varchar(200),
    hisminsa varchar(50),
    camas varchar(10),
    diris varchar(50),
    codigo varchar(20),
    sadj varchar(50),
    ambito_esn varchar(50),
    condicion varchar(50),
    sec_codigo varchar(50),
    sec_descripcion varchar(150),
    id_establecimiento varchar(20),
    est_nombre varchar(150),
    escenario varchar(50),
    pmhyf varchar(100),
    ambiental varchar(100),
    vraem varchar(10),
    frontera varchar(10),
    frontera_pais varchar(50),
    csmc varchar(10),
    grupsector varchar(50),
    terrisect varchar(50),
    grupo_diris varchar(50),
    grupo_sector varchar(50),
    sector_grupo varchar(50),
    dpto_distribu varchar(100)
);

BULK INSERT Renaes
FROM 'D:\001 DIRESA 0001\Renaes_20250320.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '|',
    ROWTERMINATOR = '\n',
    CODEPAGE = 'ACP'
);

select top 400 * from Renaes
----------------------------------------------------------------------------------
--FED_2024_25_EESS_POBLACION_ASIGNADA_20250221.csv
--FED_EESS_POBLACION_ASIGNADA

CREATE TABLE FED_EESS_POBLACION_ASIGNADA (
    region varchar(100),
    ambito varchar(100),
    categoria varchar(100),
    codigo_unico varchar(20),
    nombre_institucion varchar(255),
    estado varchar(50),
    descripcion varchar(255)
);


BULK INSERT FED_EESS_POBLACION_ASIGNADA
FROM 'D:\001 DIRESA 0001\FED_2024_25_EESS_POBLACION_ASIGNADA_20250221.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '\n',
    CODEPAGE = 'ACP'
);

select * from FED_EESS_POBLACION_ASIGNADA where region ='JUNIN'
--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------

--FED_2024_25_EESS_EXCLUIDOS_CSMC_ESPECIALIZADOS_20250128.csv
--ID_ORDEN;DIRIS_DIRESA_GERESA;PROVINCIA;DISTRITO;NOMBRE_CSMC;RENAES
--FED_2024_25_EESS_EXCLUIDOS_CSMC_ESPECIALIZADOS_20240730

CREATE TABLE FED_2024_25_EESS_EXCLUIDOS_CSMC_ESPECIALIZADOS_20240730 (
    id_orden int,
    diris_diresa_geresa varchar(150),
    provincia varchar(100),
    distrito varchar(100),
    nombre_csmc varchar(200),
    renaes varchar(20)
);

BULK INSERT FED_2024_25_EESS_EXCLUIDOS_CSMC_ESPECIALIZADOS_20240730
FROM 'D:\001 DIRESA 0001\FED_2024_25_EESS_EXCLUIDOS_CSMC_ESPECIALIZADOS_20250128.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '\n',
    CODEPAGE = 'ACP'
);