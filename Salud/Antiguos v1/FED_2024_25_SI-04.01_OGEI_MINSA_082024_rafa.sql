/*=======================================================================================================================================
Compromiso Gestion: SI-04 | Adolescentes mujeres de 12 a 17 años de edad, con dosaje de hemoglobina, en establecimientos de salud del primer y 
								segundo nivel de atención (con población asignada).

Indicador	:	SI-04.01  | Porcentaje de adolescentes mujeres de 12 a 17 años de edad, con dosaje de hemoglobina, en establecimientos de salud 
							del primer y segundo nivel de atención (con población asignada).

Elaboración	:	MINISTERIO DESARROLLO E INCLUSION SOCIAL (MIDIS)
				DIRECCIÓN DE SEGUIMIENTO Y EVALUACIÓN (DGSE)
				DIRECCIÓN DE SEGUIMIENTO (DS)
				/ JHONATAN LAVI
Revision	:	MINISTERIO DE SALUD (MINSA)
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
	--, id_tipcond_estab TipoCondicion_Personsa_Estab
	, convert(date,periodo) fecha_atencion
	, convert(int,aniomes) aniomes
	, num_doc
	, id_tipitem tipo_dx
	, cod_item  
	, valor_lab
	, edad_reg edad 
into #his_minsa
from dbo.TRAMAJESUS with (nolock)				--BD_BACKUP_OGEI.dbo.TramaHisMinsa with (nolock)
where --sw=1	and																-- Validación del numero de documento.
 id_tipo_doc=1															-- Tipo de documento: DNI.
and id_genero='F'															-- Sexo: Mujer. 
and ( convert(int,edad_reg) between 12 and 17 ) and id_tipedad_reg='A'		-- Adolescentes de 12 a 17 años de edad. 
go
select top 100 * from #his_minsa
go


-- 2.Tabla de datos : RENAES (Mes de Evaluación)
select * from #renaes
if Object_id(N'tempdb..#renaes',N'U') is not null drop table  #renaes;
select cod_estab renaes,
	   cat_estab,
	   desc_estab,
	   diris,
	   ubigeo,
	   ambito,
	   sector
into #renaes
from [dbo].[Renaes] with (nolock)
where 
(
	(	[dbo].[Renaes].tipocatego in ('I-1','I-2','I-3','I-4')		)													-- EESS de Primer Nivel. 
	or
	(	convert(int, cod_estab) in (select distinct convert(int,codigo_unico) 
							from FED_EESS_POBLACION_ASIGNADA where region ='JUNIN'   	-- EESS de Segundo Nivel con Pob Asignada.
										)
	)
) 
--and convert(int,ambito)=1	-- EESS del MINSA.
--and sw_estado=1				-- EESS Activos.


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
año				int,
mes				int,
renaes			int,
num_doc			nvarchar(15),
fecha_atendida  date,
edad_reg		int,
fecha_hb		date,
renaes_hb		int,
numerador		int,
denominador		int,
Visitas_EESs	int
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

-- 1.1 Se excluye de la trama de datos a las Adolescentes gestantes: Z3491, Z3492, Z3493, Z3591, Z3592, Z3593, Z359, Z349.
delete from #his_minsa
where num_doc in (select distinct num_doc from #his_minsa where cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z359','Z349') )   -- Gestantes Atencion Prenatal.
and year(fecha_atencion)=@año
and month(fecha_atencion)=@mes_inicio


-- 1.2  Se excluye Atenciones de las adolescentes que acuden por vacunas : COVID 90749.01 o 90749.02 , VPH 90649, Neumococo 90670, Antiamarílica 90717, Influenza 90658, Anti Hepatitis B 90746 y Diftotetanica (DT) adulto 90714.
delete from #his_minsa 
where id_cita in (select distinct id_cita from #his_minsa where cod_item in ('90749.01','90749.02'	 -- Covid
																			,'90649'				 --	VPH
																			,'90714'				 -- Vacunación Diftotetánica Adulto (dT)
																			,'90746'				 -- Vacunación Antihepatitis Viral B (Adulto (HVB)
																			,'90658'				 -- Vacunación contra la Influenza (Adulto (Estacional))
																			,'90717'				 -- Vacunación Antiamarílica (AMA) 
																			,'90670')				 -- Vacunación Antineumocócica
				)
and year(fecha_atencion)=@año
and month(fecha_atencion)=@mes_inicio


-- 1.3  Se excluye Atenciones de las IPRESS como Centros Comunitarios de Salud Mental y otros (especializados o que no brinden la atención a las adolescentes)
delete from #his_minsa 
where converT(int,renaes) in (	select distinct converT(int,renaes)
								from dbo.FED_2024_25_EESS_EXCLUIDOS_CSMC_ESPECIALIZADOS_20240730 )		-- Centros Comunitarios de Salud Mental y otros


-- 1.4  Se excluye Atenciones de telemedicina: 99499.
delete from #his_minsa 
where id_cita in	(	select distinct id_cita from #his_minsa
						where substring(cod_item,1,5)='99499' )		-- Atenciones de telemedicina


/* 1.5 - Adolescentes mujeres de 12 a 17 años de edad, atendidas (Nuevas o Reingresantes) en establecimientos de salud del primer nivel (I-1 a I-4) 
y segundo nivel de atención (con población asignada), en el mes de medición, por cualquier motivo de consulta, registrados en HIS con DNI.  */
if Object_id(N'tempdb..#denominador',N'U') is not null drop table #denominador;
select distinct a.renaes,
				a.num_doc,
				a.fecha_atencion fecha_atendida,
				a.edad
into #denominador
from #his_minsa a
inner join #renaes b on converT(int,a.renaes)=convert(int,b.renaes)
where  Id_Condicion_Establecimiento in ('N','R')						-- Adolescentes atendidas (Nuevas o Reingresantes).	
and  year(fecha_atencion)=@año
and  month(fecha_atencion)=@mes_inicio								-- Atendidas en el Mes de Medición.


select top 100 * from  TRAMAJESUS group by Id_Condicion_Establecimiento
select top 100 * from #renaes
id_tipcond_estab
--%%%%%%%%%%%%%%%%
-- 2. NUMERADOR
--%%%%%%%%%%%%%%%%

/* 2.1 Adolescentes mujeres que forman parte del denominador, que en la fecha de su atención: 
	- Cuentan con dosaje de hemoglobina *, registrado con los CÓDIGOS CPMS: 85018 o 85018.01 (tipo de diagnóstico "D").

* Para fines de búsqueda de información, se medirá que cuente con un dosaje de hemoglobina en el mes de medición  */
if Object_id(N'tempdb..#numerador',N'U') is not null drop table #numerador;
select distinct	a.renaes,
				a.num_doc,
				a.fecha_atendida,
				a.edad,
				b.fecha_atencion fecha_Hb,
				b.renaes renaes_Hb,
				num_hb=1
into #numerador
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc and a.renaes=b.renaes
where b.cod_item in ('85018','85018.01') and b.tipo_dx='D'				-- Cuentan con dosaje de hemoglobina
and b.fecha_atencion>=a.fecha_atendida									-- Se medirá que cuente con un dosaje de hemoglobina en el mes de medición
and  year(b.fecha_atencion)=@año
and  month(b.fecha_atencion)=@mes_inicio								-- Atendidas en el Mes de Medición.


--%%%%%%%%%%%%%%%%
--	3.INDICADOR
--%%%%%%%%%%%%%%%% 

--3.1 - Se procede a unir el denominador con los atributos del numerador para elaborar el reporte final. 
if Object_id(N'tempdb..#Indicador',N'U') is not null drop table #Indicador;
select a.*
, b.fecha_Hb, b.renaes_Hb, isnull(b.num_hb,0) num_hb
, max(iif(b.num_hb=1,1,0)) over (partition by a.num_doc) numerador
, denominador=1
into #Indicador
from #denominador a
left join #numerador	b on a.renaes=b.renaes and a.num_doc=b.num_doc and a.fecha_atendida=b.fecha_atendida and a.edad=b.edad
 

--======================================= Notas para Ordenar el Indicador =======================================

/* 3.2 -  En caso no haya recibido el dosaje de hemoglobina en ninguno de los establecimientos de salud que fue atendida, será contabilizada en el 
denominador del establecimiento de salud que la atendió por primera vez en el mes de medición. */
if Object_id(N'tempdb..#Indicador_den',N'U') is not null drop table #Indicador_den;
select *
, count(*) over (partition by num_doc) Visitas_EESS										-- 1.Numero de visitas de la adolescente al EESS.
, row_number() over (partition by num_doc order by fecha_atendida asc) id				-- 2.Ordenamiento el Primer EESS que atendio.
into #Indicador_den
from #Indicador
where numerador=0	-- No recibio el dosaje de hemoglobina.


/* 3.3 - En caso haya recibido el dosaje de hemoglobina en diferentes establecimientos de salud que fue atendida, será contabilizada en el 
numerador/denominador del primer establecimiento de salud que le entregó el dosaje de hemoglobina en el mes de medición.. */
if Object_id(N'tempdb..#Indicador_num',N'U') is not null drop table #Indicador_num;
select *
, count(*) over (partition by num_doc) Visitas_EESS											-- 1.Numero de visitas de la adolescente al EESS.
, row_number() over (partition by num_doc order by num_hb desc, fecha_atendida asc) id		-- 2.Ordenamiento el Primer EESS que atendio.
into #Indicador_num
from #Indicador
where numerador=1	-- ha recibido el dosaje de hemoglobina .

--=============================================================================================================================


-- 3.4 - Insertar Información a Tabla Reporte.
insert into #tabla_reporte
select	@año año,
		@mes_inicio mes,
		renaes,
		num_doc,
		fecha_atendida,
		edad,
		fecha_Hb,
		renaes_Hb,
		numerador,
		denominador,
		Visitas_EESS
from (
		select *
		from #Indicador_num where id=1		-- Trama de adolescente con dosaje de hemoglobina
		union all
		select *
		from #Indicador_den where id=1		-- Trama de adolescente sin dosaje de hemoglobina
)  as t


set @mes_inicio=@mes_inicio+1
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
if object_id(N'FED25.TRAMAHIS_FED2024_25_SI0401_Nominal', N'U') is not null drop table FED25.TRAMAHIS_FED2024_25_SI0401_Nominal;
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
into FED25.TRAMAHIS_FED2024_25_SI0401_Nominal
from #tabla_reporte a
inner join #renaes c on a.renaes=convert(int,c.renaes)
inner join MaeUbigeo_20240808 b on convert(int,c.ubigeo)=convert(int,b.ubigeo)

-- Tabla: Reporte Consolidado
select periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, eess_cat, eess_renaes, eess_nombre
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
, sum(denominador)						denominador
, sum(numerador)						numerador
from FED25.TRAMAHIS_FED2024_25_SI0401_Nominal
group by periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, eess_cat, eess_renaes, eess_nombre
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
---------------------------- =D.