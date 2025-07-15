/*=======================================================================================================================================
Compromiso Gestion: SI-01 | Gestantes con diagnóstico de anemia atendidas en establecimientos de salud del primer y segundo nivel 
					de atención (con población asignada) del MINSA, que reciben dosaje de hemoglobina de control 
					y segunda entrega de tratamiento con hierro. 

Indicador	:	SI-01.01	| Porcentaje de gestantes con diagnóstico de anemia atendidas en establecimientos de salud del primer y segundo nivel de 
				atención (con población asignada) del MINSA, que reciben dosaje de hemoglobina de control y segunda entrega 
				de tratamiento con hierro.

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
use BD_HISindicadores
go

/* **********************************************************
				Tablas.

En esta parte se procede a seleccionar las Tablas/tramas/Base de datos 
utilizadas para generar el indicador. Las Tablas/tramas/Base de datos utilizados 
para este indicador son:

- 1.HIS MINSA (Mes de evalucación) | Cambia mes a mes.
- 2.HIS MINSA (Año Previo)
- 3.RENAES (Mes de Evaluación)	   | Cambia mes a mes.
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
					,'85018','85018.01','80055.01'							-- Códigos de Dosaje de Hb/(Perfil Obstetrico)
					,'O990'													-- Códigos de Dianostico de Anemia en Gestantes
					,'99199.26','59401.04'									-- Códigos de Tratamiento de Hierro en Gestantes
				)
and sw=1			-- Validación del numero de documento.
and id_tipo_doc=1	-- Tipo de documento: DNI.
and id_genero='F'	-- Sexo: Mujer. 

-- 2.Tabla de datos : HIS MINSA (Año Previo)
insert into #his_minsa
select id_cita
	, renaes
	, convert(date,periodo) fecha_atencion
	, convert(int,aniomes) aniomes
	, num_doc
	, id_tipitem tipo_dx
	, cod_item  
	, valor_lab
from BD_BACKUP_OGEI_2024.dbo.TramaHisMinsa with (nolock)
where cod_item in (
					'Z3491','Z3591','Z3492','Z3592','Z3493','Z3593'			-- Códigos de Atención Prenatal.
					,'85018','85018.01','80055.01'							-- Códigos de Dosaje de Hb/(Perfil Obstetrico)
					,'O990'													-- Códigos de Dianostico de Anemia en Gestantes
					,'99199.26','59401.04'									-- Códigos de Tratamiento de Hierro en Gestantes
				)
and sw=1			-- Validación del numero de documento.
and id_tipo_doc=1	-- Tipo de documento: DNI.
and id_genero='F'	-- Sexo: Mujer. 

-- 3.Tabla de datos : RENAES (Mes de Evaluación)
if Object_id(N'tempdb..#renaes',N'U') is not null drop table  #renaes;
select cod_estab renaes,
	   cat_estab,
	   desc_estab,
	   diris,
	   ubigeo,
	   ambito,
	   sector
into #renaes
from BD_BACKUP_OGEI_2024.dbo.Renaes with (nolock)
where 
(
	(	cat_estab in ('I-1','I-2','I-3','I-4')		)			-- EESS de Primer Nivel. 
	or
	(	convert(int,cod_estab) in (select distinct convert(int,[CODIGO_UNICO]) 
							from [FED_2024_25_EESS_POBLACION_ASIGNADA_20240808_ANTIGUO])  ) -- EESS de Segundo Nivel con Pob Asignada.
) 
and convert(int,ambito)=1	-- EESS del MINSA.
-- and sw_estado=1				-- EESS Activos.

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- CAMBIAR LISTA DE ESTABLECIMIENTOS CON POBLACION
-- SELECT * FROM BD_HISINDICADORES.dbo.FED_2024_25_EESS_POBLACION_ASIGNADA_20240808
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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
	año							int,
	mes							int,
	renaes						int,
	fecha_atencion				date,
	fecha_corte					date,
	num_doc						nvarchar(15),
	den_apn						int,
	den_Hb						int,
	den_Dx						int,
	denominador					int,
	numerador					int,
	fecha_entregaHierro			date,
	num_entregaHierro			int,
	fecha_dosajeHb				date,
	num_dosajeHb				int
) 

declare @mes_inicio int,
		@mes_eval int,
		@año int 

set @año=2025
set @mes_inicio= 1
set @mes_eval= 1 --< Modificar segun mes de evaluación.

while @mes_inicio<=@mes_eval
begin

--%%%%%%%%%%%%%%%%
-- 1.DENOMINADOR
--%%%%%%%%%%%%%%%%

/* 1.1	-	Gestantes atendidas durante el mes previo (atendidas hace 32 días antes del mes de medición) que proceden de los Establecimiento de Salud del 
primer nivel (I-1 al I-4) y segundo nivel de atención de salud (con población asignada) del Gobierno Regional, que llegan en cualquier momento de 
la gestación y son registradas en el HIS con DNI y códigos (“Z3491” o “Z3591” o "Z3492" o "Z3592" o " Z3493" o "Z3593" ) */
if Object_id(N'tempdb..#den_apn',N'U') is not null drop table #den_apn;
select distinct a.renaes,	
				a.fecha_atencion,
				dateadd(dd,32,a.fecha_atencion) fecha_corte,
				a.num_doc,
				den_apn=1
into #den_apn
from #his_minsa a
inner join #renaes b on convert(int,a.renaes)=convert(int,b.renaes)
where cod_item in ('Z3491','Z3591','Z3492','Z3592','Z3493','Z3593')			-- Registro de Atencion Prenatal.
and	year(dateadd(dd,32,fecha_atencion))=@año								
and month(dateadd(dd,32,fecha_atencion))=@mes_inicio						-- Atendidas durante el mes previo. (Hace 32 dias)


/* 1.2	-	Gestantes que en el mismo día de la atención y en el mismo Establecimiento de Salud reciben Examen auxiliar de 
hemoglobina: 85018 o 85018.01 o 80055.01 (tipo de diagnóstico definitivo "D")  */
if Object_id(N'tempdb..#den_Hb',N'U') is not null drop table #den_Hb;
select distinct a.renaes,
				a.fecha_atencion,
				a.num_doc,
				den_hb=1
into #den_Hb
from #den_apn a
inner join #his_minsa b on convert(int,a.renaes)=convert(int,b.renaes) and a.fecha_atencion=b.fecha_atencion and a.num_doc=b.num_doc	-- Registro el mismo día de la atención y en el mismo Establecimiento de Salud
where b.cod_item in ('85018','85018.01','80055.01') and b.tipo_dx='D'																	-- Registro de dosaje de Hb


/* 1.3	-	Gestantes que en el mismo día de la atención y en el mismo Establecimiento de Salud 
Cuentan con diagnóstico de anemia: O990 (tipo de diagnóstico definitivo "D")  */
if Object_id(N'tempdb..#den_Dx',N'U') is not null drop table #den_Dx;
select distinct a.renaes,
				a.fecha_atencion,
				a.num_doc,
				den_dx=1
into #den_Dx
from #den_Hb a
inner join #his_minsa b on convert(int,a.renaes)=convert(int,b.renaes) and a.fecha_atencion=b.fecha_atencion and a.num_doc=b.num_doc	-- Registro el mismo día de la atención y en el mismo Establecimiento de Salud
where b.cod_item='O990' and b.tipo_dx='D'																								-- Registro de Dx de anemia.


/* 1.4	-	Gestantes que Iniciaron oportunamente el tratamiento con hierro a partir del diagnóstico de anemia (tipo de diagnóstico definitivo "D")
; registrado con los códigos: O990 (tipo de diagnóstico definitivo "D") y 99199.26 + Lab:1 
o O990 (tipo de diagnóstico definitivo "D") y 59401.04 + Lab:1.")  */
if Object_id(N'tempdb..#den_Dx_Trat',N'U') is not null drop table #den_Dx_Trat;
select distinct a.renaes,
				a.fecha_atencion,
				a.num_doc,
				den_dx_trat=1
into #den_Dx_Trat
from #den_Dx a
inner join #his_minsa b on convert(int,a.renaes)=convert(int,b.renaes) and a.fecha_atencion=b.fecha_atencion and a.num_doc=b.num_doc
where b.cod_item in ('99199.26','59401.04') 
and try_convert(int,b.valor_lab)=1																										-- Inicio Oportuno de tratamiento de Hierro.
and b.id_cita in (select distinct id_cita from #his_minsa
					where cod_item='O990' and tipo_dx='D' )																				-- Registro de Dx de anemia.


/* 1.5 - Unión de Información para armar el denominador del indicador. */
if Object_id(N'tempdb..#denominador',N'U') is not null drop table #denominador;
select	a.renaes,
		a.fecha_atencion,
		a.fecha_corte,
		a.num_doc,
		a.den_apn
, isnull(b.den_hb,0)	den_hb	
, isnull(c.den_dx,0)	den_dx 
, isnull(d.den_dx_trat,0) den_dx_trat
into #denominador
from #den_apn a																										-- Información de Atencion Prenatal.
left join #den_Hb			b on a.renaes=b.renaes and a.num_doc=b.num_doc and a.fecha_atencion=b.fecha_atencion	-- Información de Dosaje.
left join #den_Dx			c on a.renaes=c.renaes and a.num_doc=c.num_doc and a.fecha_atencion=c.fecha_atencion	-- Información de Diagnostico de anemia.
left join #den_Dx_Trat		d on a.renaes=d.renaes and a.num_doc=d.num_doc and a.fecha_atencion=d.fecha_atencion	-- Información de tratamiento oportuno de anemia. 


--%%%%%%%%%%%%%%%%
-- 2. NUMERADOR
--%%%%%%%%%%%%%%%%

/* 2.1 - Gestantes del denominador que Recibe una segunda entrega de hierro al mes de iniciado el tratamiento (se busca la entrega entre 28 y 32 días)
, registrado con el código 99199.26 o 59401.04 independientemente del LAB. 
		
* La entrega debe estar vinculado a un código CIE-10 de anemia O990 (tipo de diagnóstico repetitivo "R") en la misma cita y con fines de búsqueda 
de información se considerará el registro con tipo de diagnóstico definitivo "D" *
*/
if Object_id(N'tempdb..#num_entregahierro',N'U') is not null drop table #num_entregahierro;
select	a.renaes,
		a.fecha_atencion,
		a.fecha_corte,
		a.num_doc,
		max(b.fecha_atencion) fecha_entregaHierro,
		num_entregaHierro=1
into #num_entregahierro
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('99199.26','59401.04')																		-- Entrega de Segundo tratamiento de Hierro.
and b.id_cita in (select distinct id_cita from #his_minsa where cod_item='O990' and tipo_dx in ('D','R') )		-- Vinculado a un código anemia (Definitivo o repetitivo).
and ( b.fecha_atencion between dateadd(dd,28,a.fecha_atencion) and dateadd(dd,32,a.fecha_atencion) )			-- Entrega al mes de Iniciado el tratamiento (28 a 32 dias).
and b.fecha_atencion<=a.fecha_corte
and a.den_dx_trat=1
group by a.renaes,
		a.fecha_atencion,
		a.fecha_corte,
		a.num_doc


/* 2.2 - Gestantes del denominador que Cuenta con un (01) dosaje de hemoglobina de control, al mes de iniciado el tratamiento, (se busca entre 28 y 32 días)
, registrado con los códigos 85018 o 85018.01 (tipo de diagnóstico definitivo "D") */
if Object_id(N'tempdb..#num_dosajeHb',N'U') is not null drop table #num_dosajeHb;
select	a.renaes,
		a.fecha_atencion,
		a.fecha_corte,
		a.num_doc,
		max(b.fecha_atencion) fecha_dosajeHb,
		num_dosajeHb=1
into #num_dosajeHb
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('85018','85018.01') and b.tipo_dx='D'												-- Entrega de Dosaje.
and ( b.fecha_atencion between dateadd(dd,28,a.fecha_atencion) and dateadd(dd,32,a.fecha_atencion) )	-- Entrega al mes de Iniciado el tratamiento (28 a 32 dias).
and b.fecha_atencion<=a.fecha_corte
and a.den_dx_trat=1
group by a.renaes,
		a.fecha_atencion,
		a.fecha_corte,
		a.num_doc


--%%%%%%%%%%%%%%%%
--	3.INDICADOR
--%%%%%%%%%%%%%%%% 

--3.1 Se procede a unir el denominador con los atributos del numerador para elaborar el reporte final. 
if Object_id(N'tempdb..#Indicador',N'U') is not null drop table #Indicador;
select a.*
, b.fecha_entregaHierro	, isnull(b.num_entregaHierro,0) num_entregaHierro
, c.fecha_dosajeHb		, isnull(c.num_dosajeHb,0)		num_dosajeHb
, max(iif(b.num_entregaHierro=1 and c.num_dosajeHb=1,1,0)) over (partition by a.num_doc) numerador
into #Indicador
from #denominador a
left join #num_entregahierro	b on a.renaes=b.renaes and a.num_doc=b.num_doc and a.fecha_atencion=b.fecha_atencion		-- Segunda entrega de hierro.
left join #num_dosajeHb			c on a.renaes=c.renaes and a.num_doc=c.num_doc and a.fecha_atencion=c.fecha_atencion		-- Dosaje al mes de iniciado el tratamiento.
 

--======================================= Notas para Ordenar el Indicador =======================================

/* 3.2 - Si durante el mes de medición la gestante recibió el dosaje de hemoglobina de control y su segunda entrega de hierro en diferentes 
Establecimiento de Salud, esta gestante será contabilizada en el numerador del primer Establecimiento de Salud que inicio el tratamiento 
oportunamente. */
if Object_id(N'tempdb..#Indicador_num',N'U') is not null drop table #Indicador_num;
select *
, row_number() over (partition by num_doc
											order by den_dx_trat desc, fecha_atencion asc) id				-- 1.Ordenamiento el Primer EESS que atendio. %%%%%%%%% CAMBIO %%%%%%%%%
-- , row_number() over (partition by num_doc
-- 											order by fecha_atencion asc) id				-- 1.Ordenamiento el Primer EESS que atendio.
into #Indicador_num
from #Indicador
where numerador=1	-- Recibió el dosaje de hemoglobina de control y su segunda entrega de hierro.


/* 3.3 - En caso no haya recibido el dosaje de hemoglobina de control o su segunda entrega de hierro en ninguno de los establecimientos de salud que 
fue atendida, esta gestante será contabilizada en el denominador del Establecimiento de Salud que inicio el tratamiento oportunamente. */
if Object_id(N'tempdb..#Indicador_den',N'U') is not null drop table #Indicador_den;
select *
, row_number() over (partition by num_doc 
											order by den_dx_trat desc					-- 1.Ordenar por tratamiento oportuno.
													, den_dx desc						-- 2.Ordenar por diagnostico de anemia.
													, den_hb desc						-- 3.Ordenar por dosaje.
													, den_apn desc						-- 4.Ordenar por Atencion prenatal.
													, fecha_atencion asc) id			-- 5.Ordenamiento el Primer EESS que atendio.
into #Indicador_den
from #Indicador
where numerador=0	-- No ha recibido el dosaje de hemoglobina de control o su segunda entrega de hierro.

--=============================================================================================================================


-- 3.4 Insertar Información a Tabla Reporte.
insert into #tabla_reporte
select	@año año,
		@mes_inicio mes,
		renaes,
		fecha_atencion,
		fecha_corte,
		num_doc,
		den_apn,
		den_hb,
		den_dx,
		den_dx_trat,
		numerador,
		fecha_entregaHierro,
		num_entregaHierro,
		fecha_dosajeHb,
		num_dosajeHb
from (
		select *
		from #Indicador_num where id=1		-- Trama con gestantes que Recibieron el dosaje de hemoglobina de control y su segunda entrega de hierro
		union all
		select *
		from #Indicador_den where id=1		-- Trama con gestantes que No ha recibido el dosaje de hemoglobina de control o su segunda entrega de hierro.
)  as t



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
if object_id(N'FED25.TRAMAHIS_FED2024_25_SI_0101_Nominal_2025', N'U') is not null drop table FED25.TRAMAHIS_FED2024_25_SI_0101_Nominal_2025;
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
into FED25.TRAMAHIS_FED2024_25_SI_0101_Nominal_2025
from #tabla_reporte a
inner join #renaes c on a.renaes=convert(int,c.renaes)
inner join MaeUbigeo_20240808 b on convert(int,c.ubigeo)=convert(int,b.ubigeo)

-- Tabla: Reporte Consolidado
select periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, eess_cat, eess_renaes, eess_nombre
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
, sum(denominador)						denominador
, sum(numerador)						numerador
, sum(den_apn)							den_apn
, sum(den_Hb)							den_Hb
, sum(den_Dx)							den_Dx
, sum(num_entregaHierro)				num_entregaHierro
, sum(num_dosajeHb)						num_dosajeHb
-- from #TRAMAHIS_FED2024_25_SI_0101_Nominal
from FED25.TRAMAHIS_FED2024_25_SI_0101_Nominal_2025
group by periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, eess_cat, eess_renaes, eess_nombre
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
-------------------------- =D.
