/*=======================================================================================================================================
Compromiso Gestion: VI-01 | Gestantes atendidas en establecimientos de salud del primer y segundo nivel de atención (con población 
							asignada), que cuentan con detección de violencia y tamizaje positivo de violencia 
							contra la mujer.

Indicador	:	VI-01.02  |  Porcentaje de gestantes atendidas en establecimientos de salud del primer y segundo nivel de atención (con 
							 Población asignada), que cuentan tamizaje positivo de violencia contra la mujer.
	
Elaboración	:	MINISTERIO DESARROLLO E INCLUSION SOCIAL (MIDIS)
				DIRECCIÓN DE SEGUIMIENTO Y EVALUACIÓN (DGSE)
				DIRECCIÓN DE SEGUIMIENTO (DS)
				/ JHONATAN LAVI
Revisión	:	MINISTERIO DE SALUD (MINSA)
				OFICINA GENERAL DE TECNOLOGIAS DE LA INFORMACION (OGTI)
				OFICINA DE GESTION DE LA INFORMACION (OGEI)
				Wilson URVIOLA ZAPATA - ANALISTA ESTADISTICO
Fecha		:	16/08/2024
============================================================================================================================================ */

-- Especificar la Base de datos a utilizar y en donde se guarda la información. 
use BD_HISINDICADORES
go

/* **********************************************************
				Tablas.

En esta parte se procede a seleccionar las Tablas/tramas/Base de datos 
utilizadas para generar el indicador. Las Tablas/tramas/Base de datos utilizados 
para este indicador son:

- 1.HIS MINSA (Mes de evalucación) | Cambia mes a mes.
- 2.RENAES (Mes de Evaluación)	   | Cambia mes a mes.
************************************************************* */

-- 1.Tabla de datos : HIS - MINSA (Mes de evalucación)
if Object_id(N'tempdb..#his_minsa',N'U') is not null drop table #his_minsa;
select id_cita
	, renaes
	, convert(date,periodo) fecha_atencion
	, convert(int,aniomes) aniomes
	, num_doc
	, id_tipitem tipo_dx
	, cod_item  
	, valor_lab
into #his_minsa
from BD_BACKUP_OGEI.dbo.TramaHisMinsa with (nolock)
where cod_item in (
					'Z3491','Z3591','Z3492','Z3592','Z3493','Z3593'			-- Códigos de Atención Prenatal.
					,'96150.01','96150'										-- Códigos de Detección de violencia contra la mujer en gestantes.
					,'R456'													-- Códigos de Problemas relacionados con la violencia R456
					)
and sw=1			-- Validación del numero de documento.
and id_tipo_doc=1	-- Tipo de documento: DNI.
and id_genero='F'	-- Sexo: Mujer. 


-- 2.Tabla de datos : RENAES (Mes de Evaluación)
if Object_id(N'tempdb..#renaes',N'U') is not null drop table  #renaes;
select cod_estab renaes,
	   cat_estab,
	   desc_estab,
	   diris,
	   ubigeo,
	   ambito,
	   sector
into #renaes
from BD_BACKUP_OGEI.dbo.Renaes   with (nolock)
where 
(
	(	cat_estab in ('I-1','I-2','I-3','I-4')		)													-- EESS de Primer Nivel. 
	or
	(	convert(int,cod_estab) in (select distinct convert(int,[CODIGO_UNICO]) 
							from [FED_2024_25_EESS_POBLACION_ASIGNADA_20240808])  )	-- EESS de Segundo Nivel con Pob Asignada.
) 
and convert(int,ambito)=1	-- EESS del MINSA.
and sw_estado=1				-- EESS Activos.


/* *************************************************************************************
				........................
					TABLA TEMPORAL
				.......................
En esta parte se procede a generar una tabla temporal donde 
se van a insertar los valores del indicador.
El proceso de la sintaxis es un loop que va corriendo el indicador
mes a mes , para eso se debe colocar en:	
	- @mes_eval : El mes donde inicia la busqueda de información del indicador.
	- @mes_final: El mes donde finaliza la busqueda de información (Periodo de evaluación).

					................
						SINTAXIS
					................
Tabmien en esta parte se procede a generar el código que busca construir el denominador y numerador 
del indicador propuesto a partir de las especificaciones en la Ficha Tecnica. 
****************************************************************************************** */

--1. Generación de la tabla temporal.
if Object_id(N'tempdb..#tabla_reporte',N'U') is not null drop table #tabla_reporte;
create table #tabla_reporte
(
año					int,
mes					int,
renaes				int,
fecha_1atenapn		date,
num_doc				nvarchar(15),
den_apn				int,
den_Vio1			int,
den_Vio2			int,
num_Rx_Vio1			int,
num_Rx_Vio2			int,
numerador			int,
denominador			int
)

declare @mes_inicio int,
		@mes_eval int,
		@año int 

set @año=2024
set @mes_inicio=1
set @mes_eval=9 --< Modificar segun mes de evaluación.

while @mes_inicio<=@mes_eval
begin

--%%%%%%%%%%%%%%%%
-- 1.DENOMINADOR
--%%%%%%%%%%%%%%%%

/* 1.1 - Gestante atendidas en el mes de medición, en Establecimiento de Salud del primer nivel (I-1 al I-4) y del segundo nivel de atención en 
		salud (con población asignada), con detección de violencia contra la mujer, registrados en el HIS, que cumplen con 
		los siguientes criterios con los códigos CIE-10: “Z3491”o “Z3492”o “Z3493”o “Z3591”o “Z3592” o “Z3593” */
if Object_id(N'tempdb..#den_apn',N'U') is not null drop table #den_apn;
select distinct	a.renaes,
				a.fecha_atencion	fecha_1atenapn, 
				a.num_doc,
				den_apn=1
into #den_apn
from #his_minsa a		
inner join #renaes b on converT(int,a.renaes)=convert(int,b.renaes)
where a.cod_item in ('Z3491','Z3591','Z3492','Z3592','Z3493','Z3593')			-- Registro de Atención Prenatal.											
and year(a.fecha_atencion)=@año
and month(a.fecha_atencion)=@mes_inicio


/* 1.2 - Gestantes del denominador registrados en el HIS que cumplen el siguiente criterio, el mismo día de la atención prenatal y en el mismo 
Establecimiento de Salud: 
1. Detección de violencia contra la mujer en gestantes: 
- Código CPMS 96150.01 (Tipo de diagnóstico Definitivo "D")  */
if Object_id(N'tempdb..#den_Vio1',N'U') is not null drop table #den_Vio1;
select distinct	a.renaes,
				a.fecha_1atenapn, 
				a.num_doc,
				den_vio1=1
into #den_Vio1
from #den_apn a		
inner join #his_minsa b on a.fecha_1atenapn=b.fecha_atencion and a.renaes=b.renaes and a.num_doc=b.num_doc
where b.cod_item='96150.01' and b.tipo_dx='D'										-- Registro de Detección de violencia contra la mujer en gestantes ['96150.01']


/* 1.3 - Gestantes del denominador registrados en el HIS que cumplen el siguiente criterio, el mismo día de la atención prenatal y en el mismo 
Establecimiento de Salud: 
1. Detección de violencia contra la mujer en gestantes: 
- Código CPMS 96150 (Tipo de diagnóstico Definitivo "D") con LAB: VIF para fines de busqueda de datos.  */
if Object_id(N'tempdb..#den_Vio2',N'U') is not null drop table #den_Vio2;
select distinct	a.renaes,
				a.fecha_1atenapn, 
				a.num_doc,
				den_vio2=1
into #den_Vio2
from #den_apn a		
inner join #his_minsa b on a.fecha_1atenapn=b.fecha_atencion and a.renaes=b.renaes and a.num_doc=b.num_doc
where b.cod_item='96150' and b.tipo_dx='D' and b.valor_lab='VIF'					-- Registro de Detección de violencia contra la mujer en gestantes ['96150' con Valor Lab:VIF ]		


/* 1.5 - Unión de Información para armar el denominador del indicador. */
if Object_id(N'tempdb..#denominador',N'U') is not null drop table #denominador;
select a.renaes
, a.num_doc
, a.fecha_1atenapn
, a.den_apn
, isnull(b.den_vio1,0)	den_vio1
, isnull(c.den_vio2,0)	den_vio2
, iif(b.den_vio1=1 or c.den_vio2=1,1,0) denominador
into #denominador
from #den_apn a
left join #den_Vio1	b	on	a.num_doc=b.num_doc and a.fecha_1atenapn=b.fecha_1atenapn and a.renaes=b.renaes
left join #den_Vio2	c	on	a.num_doc=c.num_doc and a.fecha_1atenapn=c.fecha_1atenapn and a.renaes=c.renaes	


--%%%%%%%%%%%%%%%%
-- 2. NUMERADOR
--%%%%%%%%%%%%%%%%

/* 2.1 - Gestante del denominador que cumplen el siguiente criterio, el mismo día de la atención prenatal y en la misma Establecimiento de Salud.
 - Tamizaje positivo: Problemas relacionados con la violencia R456 (Tipo de diagnóstico Definitivo "D") | Registro de Detección de violencia contra la mujer en gestantes ['96150.01']   */
if Object_id(N'tempdb..#num_Rx_Vio1',N'U') is not null drop table #num_Rx_Vio1;
select distinct	a.renaes,
				a.fecha_1atenapn, 
				a.num_doc,
				num_Rx_Vio1=1
into #num_Rx_Vio1
from #den_Vio1 a		
inner join #his_minsa b on a.fecha_1atenapn=b.fecha_atencion and a.renaes=b.renaes and a.num_doc=b.num_doc
where b.cod_item='R456' and b.tipo_dx='D'										-- Problemas relacionados con la violencia R456 | Registro de Detección de violencia contra la mujer en gestantes ['96150.01']


/* 2.2 - Gestante del denominador que cumplen el siguiente criterio, el mismo día de la atención prenatal y en la misma Establecimiento de Salud.
 - Tamizaje positivo: Problemas relacionados con la violencia R456 (Tipo de diagnóstico Definitivo "D") | Registro de Detección de violencia contra la mujer en gestantes ['96150' con Valor Lab:VIF ]	   */
if Object_id(N'tempdb..#num_Rx_Vio2',N'U') is not null drop table #num_Rx_Vio2;
select distinct	a.renaes,
				a.fecha_1atenapn, 
				a.num_doc,
				num_Rx_Vio2=1
into #num_Rx_Vio2
from #den_Vio2 a		
inner join #his_minsa b on a.fecha_1atenapn=b.fecha_atencion and a.renaes=b.renaes and a.num_doc=b.num_doc
where b.cod_item='R456' and b.tipo_dx='D'										-- Problemas relacionados con la violencia R456 | Registro de Detección de violencia contra la mujer en gestantes ['96150' con Valor Lab:VIF ]		


--%%%%%%%%%%%%%%%%
--	3.INDICADOR
--%%%%%%%%%%%%%%%% 

-- 3.1 - Se procede a unir el denominador con los atributos del numerador para elaborar el reporte final e Insertar Información a Tabla Reporte. 
insert into #tabla_reporte
select @año año
, @mes_inicio mes
, a.renaes
, a.fecha_1atenapn
, a.num_doc
, isnull(a.den_apn,0)		den_apn	
, isnull(a.den_vio1,0)		den_vio1
, isnull(a.den_vio2,0)		den_vio2
, isnull(b.num_Rx_Vio1,0)	num_Rx_Vio1
, isnull(c.num_Rx_Vio2,0)	num_Rx_Vio2
, iif(b.num_Rx_Vio1=1 or c.num_Rx_Vio2=1,1,0) numerador
, isnull(denominador,0)		denominador
from #denominador a
left join #num_Rx_Vio1	b	on	a.num_doc=b.num_doc and a.fecha_1atenapn=b.fecha_1atenapn and a.renaes=b.renaes
left join #num_Rx_Vio2	c	on	a.num_doc=c.num_doc and a.fecha_1atenapn=c.fecha_1atenapn and a.renaes=c.renaes

set @mes_inicio = @mes_inicio + 1
end

/* ************************************************************************
					REPORTE

Reporte Nominal: Se genera el reporte nominal que se guarda en SUMATIKA.
Reporte Consolidado: Se genera el reporte consolidado que se guarda en SUMATIKA
					 y que ademas se guarda como txt para subirlo en el 
					 servidor de MIDIS para Reportes. 

*** Cada reporte es un tabla nueva, por lo tanto cambiar los ultimos 6 digitos del nombre de la tabla
	por el periodo de evaluacion (Ejemplo: si es mayo entonces es _202306, si es octubre es _202312
	, si es febrero del otro año entonces 2024_02)
*************************************************************************** */

-- Tabla: Reporte Nominal
if object_id(N'FED25.TRAMAHIS_FED2024_25_VI0102_Nominal', N'U') is not null drop table FED25.TRAMAHIS_FED2024_25_VI0102_Nominal;
select convert(char(4),año)+right('00'+convert(nvarchar(2),mes),2) Periodo
, c.UBIGEO ubigeo_inei
, b.GR_DIRIS diresa
,case
		When left(b.des_pro,4) = '1501' and b.des_dpo = '15 LIMA' then '15 LIMA METROPOLITANA'
		When b.des_dpo = '15 LIMA'  and left(b.des_pro,4) <>'1501' then '15 LIMA PROVINCIAS'
	Else b.des_dpo  End as departamento
, b.DES_PRO provincia
, b.DES_DIS distrito
, c.CAT_ESTAB eess_cat
, a.renaes eess_renaes
, c.DESC_ESTAB eess_nombre
, isnull(b.fed,0) Distritos_FED_23_24
, isnull(b.fed_limametro,0) Distritos_FED_23_24_LimaMetro 
, isnull(b.Fed_2018,0) Distritos_FED_24_25
, isnull(b.fed_IC,0) Distritos_FED_IC
, a.*
into FED25.TRAMAHIS_FED2024_25_VI0102_Nominal
from #tabla_reporte a
inner join #renaes c on a.renaes=convert(int,c.renaes)
inner join MaeUbigeo_20240808 b on convert(int,c.ubigeo)=convert(int,b.ubigeo)

-- Tabla: Reporte Consolidado
select periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, eess_cat, eess_renaes, eess_nombre
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
, sum(denominador)						denominador
, sum(numerador)						numerador
, sum(den_apn)							den_apn
, sum(den_Vio1)							den_Vio1
, sum(den_Vio2)							den_Vio2
, sum(num_Rx_Vio1)						num_Rx_Vio1
, sum(num_Rx_Vio2)						num_Rx_Vio2
from FED25.TRAMAHIS_FED2024_25_VI0102_Nominal
group by periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, eess_cat, eess_renaes, eess_nombre
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
-------------------------- =D.