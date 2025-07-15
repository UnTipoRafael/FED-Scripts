USE [BD_HISINDICADORES]

drop table dbo.Agua

CREATE TABLE dbo.Agua (
    Departamento VARCHAR(100),
    Provincia VARCHAR(100),
    Distrito VARCHAR(100),
    CentroPoblado_Ubigeo VARCHAR(20),
    CentroPoblado_Nombre VARCHAR(150),
    Distrito_Ubigeo VARCHAR(20),
    Encuesta_ID INT,
    Municipalidad VARCHAR(150),
    Municipalidad_Clasificacion VARCHAR(100),
    EncuestaModulo_ID INT,
    EncuestaModulo_IDPadre INT,
    FechaCreacion varchar(50),
    FechaModificacion DATETIME,
    MarcoModificacion VARCHAR(100),
    Sistema_Nombre VARCHAR(200),
    Sistema_EstadoInfraestructura VARCHAR(100),
    EsUltimoReporte varchar(20),
    ReporteNumero varchar(20),
    Reservorio_FechaMuestreo varchar(50),
    Reservorio_ValorCloroResidual DECIMAL(5,2),
    PrimeraVivienda_FechaMuestreo varchar(50),
    PrimeraVivienda_ValorCloroResidual DECIMAL(5,2),
    PrimeraVivienda_DNIDelTitular VARCHAR(15),
    PrimeraVivienda_NombresDelTitular VARCHAR(150),
    ViviendaIntermedia_FechaMuestreo varchar(50),
    ViviendaIntermedia_ValorCloroResidual DECIMAL(5,2),
    ViviendaIntermedia_DNIDelTitular VARCHAR(15),
    ViviendaIntermedia_NombresDelTitular VARCHAR(150),
    UltimaVivienda_FechaMuestreo varchar(50),
    UltimaVivienda_ValorCloroResidual DECIMAL(5,2),
    UltimaVivienda_DNIDelTitular VARCHAR(15),
    UltimaVivienda_NombresDelTitular VARCHAR(150),
    p01_CantidadDocumento INT,
    ViviendasHabitadasConexion INT,
    PoblacionAtendidaConexion INT,
    CumpleConAdecuadaConcentracion VARCHAR(10),
    ObservacionesGenerales VARCHAR(MAX),
    FechaProcesado varchar(50),
    ColumnaVacia VARCHAR(10) -- Si hay una columna vacía final
);

BULK INSERT dbo.Agua
FROM 'C:\Users\PC01\Documents\FED\agua.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '\n',
    CODEPAGE = '65001', -- UTF-8
    TABLOCK
);
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------

USE [BD_HISINDICADORES];  -- Reemplaza con el nombre real de tu base de datos

drop table dbo.PadronLimpio
CREATE TABLE dbo.PadronLimpio (
    Nro INT,
    TipoDocumentoNino VARCHAR(10),
    CodigoPadronNominal VARCHAR(50),
    NumeroCertificadoNV VARCHAR(50),
    CUI VARCHAR(20),
    DNI VARCHAR(15),
    EstadoTramiteDNI VARCHAR(50),
    FechaTramiteDNI  VARCHAR(12),
    ApellidoPaternoNino VARCHAR(100),
    ApellidoMaternoNino VARCHAR(100),
    NombresNino VARCHAR(150),
    SexoNino VARCHAR(1),
    FechaNacimientoNino varchar(50),
    FechaFin varchar(50),
    EdadAnio INT,
    EdadMes INT,
    EdadDia INT,
    Datos VARCHAR(MAX),
    EdadTexto VARCHAR(100),
    EjeVial VARCHAR(100),
    Descripcion VARCHAR(200),
    ReferenciaDireccion VARCHAR(200),
    CodigoUbigeoDistrito VARCHAR(10),
    Departamento VARCHAR(100),
    Provincia VARCHAR(100),
    Distrito VARCHAR(100),
    CodigoCentroPoblado VARCHAR(50),
    NombreCentroPoblado VARCHAR(150),
    AreaCentroPoblado VARCHAR(100),
    MenorVisitado VARCHAR(50),
    MenorEncontrado VARCHAR(50),
    FechaVisita varchar(50),
    FuenteDatos VARCHAR(100),
    FechaFuenteDatos varchar(50),
    CodigoEESSNacimiento VARCHAR(50),
    NombreEESSNacimiento VARCHAR(150),
    CodigoEESS VARCHAR(50),
    NombreEESS VARCHAR(150),
    FrecuenciaAtencion VARCHAR(50),
    CodigoEESSAdscripcion VARCHAR(50),
    NombreEESSAdscripcion VARCHAR(150),
    TipoSeguro VARCHAR(50),
    ProgramasSociales VARCHAR(50),
    CodigoIE VARCHAR(50),
    NombreIE VARCHAR(150),
    RelacionFamiliar VARCHAR(10),
    TipoDocumentoMadre VARCHAR(20),
    DNIMadre VARCHAR(15),
    ApellidoPaternoMadre VARCHAR(100),
    ApellidoMaternoMadre VARCHAR(100),
    NombresMadre VARCHAR(150),
    CelularMadre VARCHAR(20),
    CorreoMadre VARCHAR(150),
    GradoInstruccionMadre VARCHAR(100),
    LenguaMadre VARCHAR(100),
    RelacionJefeFamilia VARCHAR(10),
    TipoDocumentoJefe VARCHAR(20),
    DNIJefe VARCHAR(20),
    ApellidoPaternoJefe VARCHAR(100),
    ApellidoMaternoJefe VARCHAR(100),
    NombresJefe VARCHAR(150),
    EstadoRegistro VARCHAR(10),
    FechaCreacion varchar(50),
    UsuarioCrea VARCHAR(100),
    FechaModificacion varchar(100),
    UsuarioModifica VARCHAR(100),
    Entidad VARCHAR(100),
    TipoRegistro VARCHAR(50),
    Matriculados VARCHAR(10),
    NoMatriculados VARCHAR(10)
);
----------------
BULK INSERT dbo.PadronLimpio
FROM 'C:\Users\PC01\Downloads\FED Datos\padron nominal v1 31mayo-2024 v2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '\n',
    CODEPAGE = 'ACP', -- '65001', -- Para UTF-8
    TABLOCK
);
---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------


use [BD_HISINDICADORES]

drop table dbo.SiagieMatriculados

CREATE TABLE dbo.SiagieMatriculados (
    dni VARCHAR(15),
    ti_doc_identidad VARCHAR(10),
    ubigeo VARCHAR(10),
    departamento VARCHAR(100),
    provincia VARCHAR(100),
    distrito VARCHAR(100),
    co_centro_poblado VARCHAR(50),
    de_centro_poblado VARCHAR(150),
    de_genero_menor VARCHAR(10),
    fe_nac_menor DATE,
    cod_mod VARCHAR(20),
    fecha_registro varchar(20),
    estado_matricula VARCHAR(10),
    dsc_estado_matricula VARCHAR(100),
    id_grado VARCHAR(10),
    niv_mod VARCHAR(10),
    d_niv_mod VARCHAR(100),
    modalidad VARCHAR(100),
    fecha_nac2 varchar(20),
    fecha_ref varchar(20),
    edad INT,
    matriculado INT,
    no_matriculado INT,
    total INT
);

---
BULK INSERT dbo.SiagieMatriculados
FROM 'C:\Users\PC01\Downloads\FED Datos\SIAGIE-Matriculados.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '\n',
    CODEPAGE = 'ACP',  -- ANSI
    TABLOCK
);
-----

--SiagieMatriculados
--PadronLimpio

use [BD_HISINDICADORES]

select PadronLimpio.*, SiagieMatriculados.matriculado datos  from PadronLimpio inner join SiagieMatriculados
	--on PadronLimpio.DNI= SiagieMatriculados.dni
	ON CAST(PadronLimpio.DNI AS VARCHAR(20)) = CAST(SiagieMatriculados.dni AS VARCHAR(20))

	where 
		SiagieMatriculados.matriculado=1
		and PadronLimpio.DNI is not null
		and Datos like '"3 años%'
		and Datos not like '"3 años, 11 meses, 30 días"'

		order by PadronLimpio.Datos asc
--------
--Numerador
select PadronLimpio.Provincia, PadronLimpio.Distrito,  count(*) as total  from PadronLimpio inner join SiagieMatriculados
	--on PadronLimpio.DNI= SiagieMatriculados.dni
	ON CAST(PadronLimpio.DNI AS VARCHAR(20)) = CAST(SiagieMatriculados.dni AS VARCHAR(20))

	where 
		SiagieMatriculados.matriculado=1
		and PadronLimpio.DNI is not null
		and Datos like '"3 años%'
		and Datos not like '"3 años, 11 meses, 30 días"'
		and NombreIE IS NOT NULL or CodigoIE is not null
		group by  PadronLimpio.Provincia, PadronLimpio.Distrito
		order by distrito

SELECT NombreIE FROM PadronLimpio where NombreIE is not null

select PadronLimpio.Provincia, PadronLimpio.Distrito from  PadronLimpio
where Distrito like 'Yau%'
group by provincia, distrito
order by Provincia	asc , distrito asc
----------------

