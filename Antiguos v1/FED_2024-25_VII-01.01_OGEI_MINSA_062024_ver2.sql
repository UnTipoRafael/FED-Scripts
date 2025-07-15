/*=======================================================================================================================================
Compromiso Gestion: VII-01 | Gestantes atendidas en establecimientos de salud del primer y segundo nivel de atención (con población asignada) 
							, con diagnóstico de violencia, que reciben un paquete mínimo de intervenciones terapeuticas especializadas.

Indicador	:	VII-01.01  |  Porcentaje de gestantes atendidas en establecimientos de salud del primer y segundo nivel de atención (con 
							población asignada), con diagnóstico de violencia, que reciben un paquete mínimo de intervenciones terapeuticas especializadas
	
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
- 2.HIS MINSA (Año Previo)
- 3.CNV (Mes de Evaluación)		   | Cambia mes a mes.
- 4.RENAES (Mes de Evaluación)	   | Cambia mes a mes.
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
where (
(cod_item in (
					'Z3491','Z3591','Z3492','Z3592','Z3493','Z3593'			-- Códigos de Atención Prenatal.
					,'96150.01','96150'										-- Códigos de Detección de violencia contra la mujer en gestantes.
					,'R456'													-- Códigos de Problemas relacionados con la violencia.
					,'t740'													-- Códigos de Negligencia y abandono
					,'t741'													-- Códigos de Abuso físico
					,'t742'													-- Códigos de Abuso sexual
					,'t743'													-- Códigos de Abuso psicológico
					,'t748'													-- Códigos de Otros síndromes del maltrato (formas mixtas).
					,'t749'													-- Códigos de Síndrome de maltrato, no especificado
					,'99207','99214.06','99215'								-- Códigos de Consultas de salud mental:
					,'99207.01','90806','90834','90860'						-- Códigos de Psicoterapias individuales de salud mental
				)	)
or 
	( substring(cod_item,1,3) between 'Y04' and 'Y08' )						-- Códigos de Agresiones.
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
from BD_BACKUP_OGEI_2023.dbo.TramaHisMinsa with (nolock)
where (
(cod_item in (
					'Z3491','Z3591','Z3492','Z3592','Z3493','Z3593'			-- Códigos de Atención Prenatal.
					,'96150.01','96150'										-- Códigos de Detección de violencia contra la mujer en gestantes.
					,'R456'													-- Códigos de Problemas relacionados con la violencia.
					,'t740'													-- Códigos de Negligencia y abandono
					,'t741'													-- Códigos de Abuso físico
					,'t742'													-- Códigos de Abuso sexual
					,'t743'													-- Códigos de Abuso psicológico
					,'t748'													-- Códigos de Otros síndromes del maltrato (formas mixtas).
					,'t749'													-- Códigos de Síndrome de maltrato, no especificado
					,'99207','99214.06','99215'								-- Códigos de Consultas de salud mental:
					,'99207.01','90806','90834','90860'						-- Códigos de Psicoterapias individuales de salud mental
				)	)
or 
	( substring(cod_item,1,3) between 'Y04' and 'Y08' )						-- Códigos de Agresiones.
	)
and sw=1			-- Validación del numero de documento.
and id_tipo_doc=1	-- Tipo de documento: DNI.
and id_genero='F'	-- Sexo: Mujer. 


-- 3.Tabla de datos : CNV (Mes de evaluación)
if Object_id(N'tempdb..#cnv',N'U') is not null drop table #cnv;
select distinct nu_doc_madre  num_doc,
				convert(date,fe_nacido) fecha_parto
into #cnv
from BD_BACKUP_OGEI.dbo.TramaCNV with (nolock)
where convert(int,tip_doc_madre)=1		-- Tipo de documento: DNI
and sw_dni=1							-- Validación del numero de documento.

		--------------------------------------------
		--- Eliminar registros vacios o nulos ---
		delete from #cnv
		where num_doc is null or num_doc=''


-- 4.Tabla de datos : RENAES (Mes de Evaluación)
if Object_id(N'tempdb..#renaes',N'U') is not null drop table  #renaes;
select cod_estab renaes,
	   cat_estab,
	   desc_estab,
	   diris,
	   ubigeo,
	   ambito,
	   sector,
	   iif(cat_estab<>'I-1',1,0) Flag_renaes	 -- Establecimiento de Salud del primer nivel de atención de salud (I-2 al I-4) e Establecimiento de Salud del segundo nivel de atención con población asignadA
into #renaes
from BD_BACKUP_OGEI.dbo.Renaes with (nolock)
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
año										int,
mes										int,
renaes_Dx								int,
renaes_APN								int,
fecha_atenAPN							date,
num_doc									nvarchar(15),
fecha_DX								date,
fecha_Dx_3m								date,
fecha_Dx_6m								date,
filtro									nvarchar(50),
fecha_CSM1								date,
num_CSM1								int,
fecha_CSM2								date,
num_CSM2								int,
fecha_Psico1							date,
num_Psico1								int,
fecha_Psico2							date,
num_Psico2								int,
fecha_Psico3							date,
num_Psico3								int,
fecha_Psico4							date,
num_Psico4								int,
fecha_Psico5							date,
num_Psico5								int,
fecha_Psico6							date,
num_Psico6								int,
numerador_3m							float,
numerador								float,
denominador								float
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

/* 1.1 - Gestantes de Establecimiento de Salud del primer nivel de atención de salud (I-1 al I-4) y Establecimiento de Salud del segundo nivel de atención con población asignada
(incluye los Centros de Salud Mental Comunitarios), que cumplen los siguientes  criterios:
1. Cuentan con un registro en el HIS de las Atenciones de gestantes en Establecimiento de Salud, con los códigos CIE-10 “Z3491”o “Z3492”o  “Z3493”o “Z3591”o “Z3592” o “Z3593”. */
if Object_id(N'tempdb..#den_apn',N'U') is not null drop table #den_apn;
select distinct	a.renaes,
				a.fecha_atencion	fecha_1atenapn, 
				a.num_doc
into #den_apn
from #his_minsa a		
inner join #renaes b on converT(int,a.renaes)=convert(int,b.renaes)
where a.cod_item in ('Z3491','Z3591','Z3492','Z3592','Z3493','Z3593')					-- Registro de Atención Prenatal.											


/* 1.2 - Gestantes de Establecimiento de Salud del primer nivel de atención de salud (I-1 al I-4) y Establecimiento de Salud del segundo nivel de atención con población asignada
(incluye los Centros de Salud Mental Comunitarios), que cumplen los siguientes  criterios:
1.  Detección de violencia contra la mujer en gestantes: Código CPMS 96150.01 (Tipo de diagnostico definitivo "D") o 96150 Tipo de diagnostico  definitivo "D") y LAB: VIF. */
if Object_id(N'tempdb..#den_vio',N'U') is not null drop table #den_vio;
select distinct	a.renaes,
				a.fecha_1atenapn, 
				a.num_doc
into #den_vio
from #den_apn a		
inner join #his_minsa b on converT(int,a.renaes)=convert(int,b.renaes) and a.num_doc=b.num_doc and a.fecha_1atenapn=b.fecha_atencion
where 
(
(b.cod_item='96150.01' and b.tipo_dx='D')										-- Detección de violencia contra la mujer en gestantes  [CPMS 96150.01]
or
(b.cod_item='96150' and b.valor_lab='VIF' and b.tipo_dx='D')					-- Detección de violencia contra la mujer en gestantes  [CPMS 96150 and valor=VIF]
)


/* 1.3 - Gestantes de Establecimiento de Salud del primer nivel de atención de salud (I-1 al I-4) y Establecimiento de Salud del segundo nivel de atención con población asignada
(incluye los Centros de Salud Mental Comunitarios), que cumplen los siguientes  criterios:
1.  Tamizaje positivo: Problemas relacionados con la violencia R456 (D: Definitivo). */
if Object_id(N'tempdb..#den_vio_pos',N'U') is not null drop table #den_vio_pos;
select distinct	a.renaes,
				a.fecha_1atenapn, 
				a.num_doc
into #den_vio_pos
from #den_vio a		
inner join #his_minsa b on converT(int,a.renaes)=convert(int,b.renaes) and a.num_doc=b.num_doc and a.fecha_1atenapn=b.fecha_atencion
where b.cod_item='R456' and b.tipo_dx='D'										-- Tamizaje positivo


/* 1.4 -  Diagnóstico Definitivo (D) hasta 15 días después del tamizaje positivo, registrados, en Establecimiento de Salud del primer nivel de atención 
de salud (I-2 al I-4) e Establecimiento de Salud del segundo nivel de atención con población asignada, según esquema, 
con los siguientes códigos CIE-10:
- T74.0 Negligencia y abandono
- T74.1 Abuso físico.
- T74.2 Abuso sexual.
- T74.3 Abuso psicológico
- T74.8 Otros síndromes del maltrato (formas mixtas).
- T74.9 Síndrome de maltrato, no especificado
- Y04 hasta Y08: Agresiones																					*/
if Object_id(N'tempdb..#Den_Tam_Dx',N'U') is not null drop table #Den_Tam_Dx;
select distinct	b.renaes	renaes_Dx,
				a.renaes	renaes_apn,
				a.fecha_1atenapn,
				a.num_doc,
				b.fecha_atencion fecha_dx
into #Den_Tam_Dx
from #den_vio_pos a
inner join #his_minsa	b on a.num_doc=b.num_doc
inner join #renaes		c on convert(int,b.renaes)=convert(int,c.renaes)
where (
(cod_item in (		't740'													-- Negligencia y abandono
					,'t741'													-- Abuso físico
					,'t742'													-- Abuso sexual
					,'t743'													-- Abuso psicológico
					,'t748'													-- Otros síndromes del maltrato (formas mixtas).
					,'t749'													-- Síndrome de maltrato, no especificado
				)	)
or 
	( substring(cod_item,1,3) between 'Y04' and 'Y08' )						-- Agresiones.
)
and b.tipo_dx='D'
and (b.fecha_atencion between a.fecha_1atenapn and dateadd(dd,15,a.fecha_1atenapn))		-- hasta 15 días después del tamizaje positivo
and c.Flag_renaes=1																		-- Establecimiento de Salud del primer nivel de atención de salud (I-2 al I-4) e Establecimiento de Salud del segundo nivel de atención con población asignadA


/* 1.5 - Ordenar la trama del denominador por fecha de dx y Espacio para la entrega del paquete de tratamiento.
-  1. Paquete a los 3 meses (119 dias).
-  2. Paquete a los 6 meses (209 dias).  */
if Object_id(N'tempdb..#denominador',N'U') is not null drop table #denominador;
select distinct renaes_dx, 
				renaes_apn, 
				fecha_1atenapn, 
				num_doc, 
				fecha_dx, 
				fecha_dx_3m, 
				fecha_dx_6m, 
				filtro, 
				denominador=1
into #denominador
from (

		select distinct renaes_dx, renaes_apn, fecha_1atenapn, num_doc, fecha_dx, dateadd(dd,119,fecha_dx) fecha_dx_3m, fecha_dx_6m=null, filtro='1. Dx de violencia hace 3 meses'
		from #Den_Tam_Dx 
		where year(dateadd(dd,119,fecha_dx))=@año
		and month(dateadd(dd,119,fecha_dx))=@mes_inicio			-- DX los 3 meses (119 dias).

		union all

		select distinct renaes_dx, renaes_apn, fecha_1atenapn, num_doc, fecha_dx, dateadd(dd,119,fecha_dx) fecha_dx_3m, dateadd(dd,209,fecha_dx) fecha_dx_6m, filtro='2. Dx de violencia hace 6 meses'
		from #Den_Tam_Dx 
		where year(dateadd(dd,209,fecha_dx))=@año
		and month(dateadd(dd,209,fecha_dx))=@mes_inicio			-- DX los 6 meses (209 dias)

) as t


/* 1.6 -  Se excluye las gestantes con partos de los últimos seis meses previos al periodo de medición. */

-- Excluye gestantes con partos en los ultimos 3 meses.
delete from #denominador
where num_doc in (
						select distinct a.num_doc
						from #denominador a
						inner join #cnv b on a.num_doc=b.num_doc
						where filtro='1. Dx de violencia hace 3 meses'
						and (b.fecha_parto between a.fecha_dx and a.fecha_dx_3m)
)
and filtro='1. Dx de violencia hace 3 meses'

-- Excluye gestantes con partos en los ultimos 6 meses.
delete from #denominador
where num_doc in (
						select distinct a.num_doc
						from #denominador a
						inner join #cnv b on a.num_doc=b.num_doc
						where filtro='2. Dx de violencia hace 6 meses'
						and (b.fecha_parto between a.fecha_dx and a.fecha_dx_6m)
)
and filtro='2. Dx de violencia hace 6 meses'


--%%%%%%%%%%%%%%%%%%%%
-- 2.NUMERADOR 
--%%%%%%%%%%%%%%%%%%%%5

/* 2.1 - Gestantes del denominador, registrados en Establecimiento de Salud del primer nivel de atención de salud (I-2 al I-4) y 
Establecimiento de Salud del segundo nivel de atención con población asignada del Gobierno Regional (incluye los Centros de Salud Mental Comunitarios), que cumplen los siguientes criterios:
* Registrados con los siguientes códigos de Consultas de salud mental: Código CPMS 99207 o 99214.06 o 99215. 

- Las prestaciones evaluadas en los puntos 1 y 2 deben registrar además algún código de diagnóstico CIE-10 con tipo de diagnóstico "Definitivo o Repetitivo": T740, T741, T742, T743, T748, T749, Y04 hasta Y08, en la cita de la prestación.
- Entre cada consulta de salud mental o psicoterapia individual habrá un intervalo mínimo de 7 días y máximo de 30 días */

-- 2.1.1 Primera consulta de salud mental. 
if Object_id(N'tempdb..#num_CSM1',N'U') is not null drop table #num_CSM1;
select distinct	a.renaes_dx, 
				a.renaes_apn, 
				a.fecha_1atenapn, 
				a.num_doc, 
				a.fecha_dx,
				a.fecha_dx_3m,
				a.fecha_dx_6m,
				filtro,
				min(b.fecha_atencion) fecha_CSM1,
				num_CSM1=1
into #num_CSM1
from #denominador a		
inner join #his_minsa b on a.num_doc=b.num_doc
inner join #renaes	  c on converT(int,b.renaes)=convert(int,c.renaes)
where b.cod_item in ('99207','99214.06','99215')							-- Consultas de salud mental:
and c.Flag_renaes=1															-- Establecimiento de Salud del primer nivel de atención de salud (I-2 al I-4) e Establecimiento de Salud del segundo nivel de atención con población asignadA
and (b.fecha_atencion between a.fecha_dx and a.fecha_dx_3m)					-- Periodo de busqueda: 3 Meses.
and b.id_cita in ( select distinct id_cita from #his_minsa
					where	(
						(cod_item in ('t740','t741','t742','t743','t748','t749') )	-- Dx de violencia.
						or ( substring(cod_item,1,3) between 'Y04' and 'Y08' )		-- Agresiones.
							) 	and b.tipo_dx in ('D','R') )
group by a.renaes_dx, a.renaes_apn, a.fecha_1atenapn, 
		a.num_doc, a.fecha_dx, a.fecha_dx_3m, a.fecha_dx_6m, filtro


-- 2.1.2 Segunda consulta de salud mental. 
if Object_id(N'tempdb..#num_csm2',N'U') is not null drop table #num_csm2;
select distinct	a.renaes_dx, 
				a.renaes_apn, 
				a.fecha_1atenapn, 
				a.num_doc, 
				a.fecha_dx,
				a.fecha_dx_3m,
				a.fecha_dx_6m,
				filtro,
				min(b.fecha_atencion) fecha_csm2,
				num_csm2=1
into #num_csm2
from #num_CSM1 a		
inner join #his_minsa b on a.num_doc=b.num_doc
inner join #renaes	  c on converT(int,b.renaes)=convert(int,c.renaes)
where b.cod_item in ('99207','99214.06','99215')											-- Consultas de salud mental:
and c.Flag_renaes=1																			-- Establecimiento de Salud del primer nivel de atención de salud (I-2 al I-4) e Establecimiento de Salud del segundo nivel de atención con población asignadA
and (b.fecha_atencion between a.fecha_dx and a.fecha_dx_3m)									-- Periodo de busqueda: 3 Meses.
and (b.fecha_atencion between dateadd(dd,7,a.fecha_CSM1) and dateadd(dd,30,a.fecha_CSM1))	-- Entre cada consulta de salud mental o psicoterapia individual habrá un intervalo mínimo de 7 días y máximo de 30 días
and b.id_cita in ( select distinct id_cita from #his_minsa
					where	(
						(cod_item in ('t740','t741','t742','t743','t748','t749') )	-- Dx de violencia.
						or ( substring(cod_item,1,3) between 'Y04' and 'Y08' )		-- Agresiones.
							) 	and b.tipo_dx in ('D','R') )
group by a.renaes_dx, a.renaes_apn, a.fecha_1atenapn, 
		a.num_doc, a.fecha_dx, a.fecha_dx_3m, a.fecha_dx_6m, filtro


/* 2.2 - Gestantes del denominador, registrados en Establecimiento de Salud del primer nivel de atención de salud (I-2 al I-4) y 
Establecimiento de Salud del segundo nivel de atención con población asignada del Gobierno Regional (incluye los Centros de Salud Mental Comunitarios), que cumplen los siguientes criterios:
* Registrados con los siguientes códigos de Psicoterapias individuales de salud mental: Código CPMS 99207.01 o 90806 o 90834 o 90860.

- Las prestaciones evaluadas en los puntos 1 y 2 deben registrar además algún código de diagnóstico CIE-10 con tipo de diagnóstico "Definitivo o Repetitivo": T740, T741, T742, T743, T748, T749, Y04 hasta Y08, en la cita de la prestación.
- Entre cada consulta de salud mental o psicoterapia individual habrá un intervalo mínimo de 7 días y máximo de 30 días */

-- 2.2.1 Primera consulta de Piscoterapia individual.
if Object_id(N'tempdb..#num_psico1',N'U') is not null drop table #num_psico1;
select distinct	a.renaes_dx, 
				a.renaes_apn, 
				a.fecha_1atenapn, 
				a.num_doc, 
				a.fecha_dx,
				a.fecha_dx_3m,
				a.fecha_dx_6m,
				filtro,
				min(b.fecha_atencion) fecha_psico1,
				num_psico1=1
into #num_psico1
from #denominador a		
inner join #his_minsa b on a.num_doc=b.num_doc
inner join #renaes	  c on converT(int,b.renaes)=convert(int,c.renaes)
where b.cod_item in ('99207.01','90806','90834','90860')					-- Consultas de Piscoterapia individual.
and c.Flag_renaes=1															-- Establecimiento de Salud del primer nivel de atención de salud (I-2 al I-4) e Establecimiento de Salud del segundo nivel de atención con población asignadA
and (b.fecha_atencion between a.fecha_dx and a.fecha_dx_3m)					-- Periodo de busqueda: 3 Meses.
and b.id_cita in ( select distinct id_cita from #his_minsa
					where	(
						(cod_item in ('t740','t741','t742','t743','t748','t749') )	-- Dx de violencia.
						or ( substring(cod_item,1,3) between 'Y04' and 'Y08' )		-- Agresiones.
							) 	and b.tipo_dx in ('D','R') )
group by a.renaes_dx, a.renaes_apn, a.fecha_1atenapn, 
		a.num_doc, a.fecha_dx, a.fecha_dx_3m, a.fecha_dx_6m, filtro

		
-- 2.2.2 Segunda consulta de Piscoterapia individual.
if Object_id(N'tempdb..#num_psico2',N'U') is not null drop table #num_psico2;
select distinct	a.renaes_dx, 
				a.renaes_apn, 
				a.fecha_1atenapn, 
				a.num_doc, 
				a.fecha_dx,
				a.fecha_dx_3m,
				a.fecha_dx_6m,
				filtro,
				min(b.fecha_atencion) fecha_psico2,
				num_psico2=1
into #num_psico2
from #num_psico1 a		
inner join #his_minsa b on a.num_doc=b.num_doc
inner join #renaes	  c on converT(int,b.renaes)=convert(int,c.renaes)
where b.cod_item in ('99207.01','90806','90834','90860')									-- Consultas de Piscoterapia individual.
and c.Flag_renaes=1																			-- Establecimiento de Salud del primer nivel de atención de salud (I-2 al I-4) e Establecimiento de Salud del segundo nivel de atención con población asignadA
and (b.fecha_atencion between a.fecha_dx and a.fecha_dx_3m)									-- Periodo de busqueda: 3 Meses.
and (b.fecha_atencion between dateadd(dd,7,a.fecha_psico1) and dateadd(dd,30,a.fecha_psico1))	-- Entre cada consulta de salud mental o psicoterapia individual habrá un intervalo mínimo de 7 días y máximo de 30 días
and b.id_cita in ( select distinct id_cita from #his_minsa
					where	(
						(cod_item in ('t740','t741','t742','t743','t748','t749') )	-- Dx de violencia.
						or ( substring(cod_item,1,3) between 'Y04' and 'Y08' )		-- Agresiones.
							) 	and b.tipo_dx in ('D','R') )
group by a.renaes_dx, a.renaes_apn, a.fecha_1atenapn, 
		a.num_doc, a.fecha_dx, a.fecha_dx_3m, a.fecha_dx_6m, filtro

		
-- 2.2.3 Tercera consulta de Piscoterapia individual.
if Object_id(N'tempdb..#num_psico3',N'U') is not null drop table #num_psico3;
select distinct	a.renaes_dx, 
				a.renaes_apn, 
				a.fecha_1atenapn, 
				a.num_doc, 
				a.fecha_dx,
				a.fecha_dx_3m,
				a.fecha_dx_6m,
				filtro,
				min(b.fecha_atencion) fecha_psico3,
				num_psico3=1
into #num_psico3
from #num_psico2 a		
inner join #his_minsa b on a.num_doc=b.num_doc
inner join #renaes	  c on converT(int,b.renaes)=convert(int,c.renaes)
where b.cod_item in ('99207.01','90806','90834','90860')									-- Consultas de Piscoterapia individual.
and c.Flag_renaes=1																			-- Establecimiento de Salud del primer nivel de atención de salud (I-2 al I-4) e Establecimiento de Salud del segundo nivel de atención con población asignadA
and (b.fecha_atencion between a.fecha_dx and a.fecha_dx_3m)									-- Periodo de busqueda: 3 Meses.
and (b.fecha_atencion between dateadd(dd,7,a.fecha_psico2) and dateadd(dd,30,a.fecha_psico2))	-- Entre cada consulta de salud mental o psicoterapia individual habrá un intervalo mínimo de 7 días y máximo de 30 días
and b.id_cita in ( select distinct id_cita from #his_minsa
					where	(
						(cod_item in ('t740','t741','t742','t743','t748','t749') )	-- Dx de violencia.
						or ( substring(cod_item,1,3) between 'Y04' and 'Y08' )		-- Agresiones.
							) 	and b.tipo_dx in ('D','R') )
group by a.renaes_dx, a.renaes_apn, a.fecha_1atenapn, 
		a.num_doc, a.fecha_dx, a.fecha_dx_3m, a.fecha_dx_6m, filtro

		
-- 2.2.4 Cuarto consulta de Piscoterapia individual.
if Object_id(N'tempdb..#num_psico4',N'U') is not null drop table #num_psico4;
select distinct	a.renaes_dx, 
				a.renaes_apn, 
				a.fecha_1atenapn, 
				a.num_doc, 
				a.fecha_dx,
				a.fecha_dx_3m,
				a.fecha_dx_6m,
				filtro,
				min(b.fecha_atencion) fecha_psico4,
				num_psico4=1
into #num_psico4
from #num_psico3 a		
inner join #his_minsa b on a.num_doc=b.num_doc
inner join #renaes	  c on converT(int,b.renaes)=convert(int,c.renaes)
where b.cod_item in ('99207.01','90806','90834','90860')										-- Consultas de Piscoterapia individual.
and c.Flag_renaes=1																				-- Establecimiento de Salud del primer nivel de atención de salud (I-2 al I-4) e Establecimiento de Salud del segundo nivel de atención con población asignadA
and (b.fecha_atencion between a.fecha_dx and a.fecha_dx_6m)										-- Periodo de busqueda: 3 Meses.
and (b.fecha_atencion between dateadd(dd,7,a.fecha_psico3) and dateadd(dd,30,a.fecha_psico3))	-- Entre cada consulta de salud mental o psicoterapia individual habrá un intervalo mínimo de 7 días y máximo de 30 días
and a.filtro='2. Dx de violencia hace 6 meses'
and b.id_cita in ( select distinct id_cita from #his_minsa
					where	(
						(cod_item in ('t740','t741','t742','t743','t748','t749') )	-- Dx de violencia.
						or ( substring(cod_item,1,3) between 'Y04' and 'Y08' )		-- Agresiones.
							) 	and b.tipo_dx in ('D','R') )
group by a.renaes_dx, a.renaes_apn, a.fecha_1atenapn, 
		a.num_doc, a.fecha_dx, a.fecha_dx_3m, a.fecha_dx_6m, filtro

		
-- 2.2.5 Quinto consulta de Piscoterapia individual.
if Object_id(N'tempdb..#num_psico5',N'U') is not null drop table #num_psico5;
select distinct	a.renaes_dx, 
				a.renaes_apn, 
				a.fecha_1atenapn, 
				a.num_doc, 
				a.fecha_dx,
				a.fecha_dx_3m,
				a.fecha_dx_6m,
				filtro,
				min(b.fecha_atencion) fecha_psico5,
				num_psico5=1
into #num_psico5
from #num_psico4 a		
inner join #his_minsa b on a.num_doc=b.num_doc
inner join #renaes	  c on converT(int,b.renaes)=convert(int,c.renaes)
where b.cod_item in ('99207.01','90806','90834','90860')										-- Consultas de Piscoterapia individual.
and c.Flag_renaes=1																				-- Establecimiento de Salud del primer nivel de atención de salud (I-2 al I-4) e Establecimiento de Salud del segundo nivel de atención con población asignadA
and (b.fecha_atencion between a.fecha_dx and a.fecha_dx_6m)										-- Periodo de busqueda: 3 Meses.
and (b.fecha_atencion between dateadd(dd,7,a.fecha_psico4) and dateadd(dd,30,a.fecha_psico4))	-- Entre cada consulta de salud mental o psicoterapia individual habrá un intervalo mínimo de 7 días y máximo de 30 días
and a.filtro='2. Dx de violencia hace 6 meses'
and b.id_cita in ( select distinct id_cita from #his_minsa
					where	(
						(cod_item in ('t740','t741','t742','t743','t748','t749') )	-- Dx de violencia.
						or ( substring(cod_item,1,3) between 'Y04' and 'Y08' )		-- Agresiones.
							) 	and b.tipo_dx in ('D','R') )
group by a.renaes_dx, a.renaes_apn, a.fecha_1atenapn, 
		a.num_doc, a.fecha_dx, a.fecha_dx_3m, a.fecha_dx_6m, filtro

		
-- 2.2.6 Sexta consulta de Piscoterapia individual.
if Object_id(N'tempdb..#num_psico6',N'U') is not null drop table #num_psico6;
select distinct	a.renaes_dx, 
				a.renaes_apn, 
				a.fecha_1atenapn, 
				a.num_doc, 
				a.fecha_dx,
				a.fecha_dx_3m,
				a.fecha_dx_6m,
				filtro,
				min(b.fecha_atencion) fecha_psico6,
				num_psico6=1
into #num_psico6
from #num_psico5 a		
inner join #his_minsa b on a.num_doc=b.num_doc
inner join #renaes	  c on converT(int,b.renaes)=convert(int,c.renaes)
where b.cod_item in ('99207.01','90806','90834','90860')										-- Consultas de Piscoterapia individual.
and c.Flag_renaes=1																				-- Establecimiento de Salud del primer nivel de atención de salud (I-2 al I-4) e Establecimiento de Salud del segundo nivel de atención con población asignadA
and (b.fecha_atencion between a.fecha_dx and a.fecha_dx_6m)										-- Periodo de busqueda: 3 Meses.
and (b.fecha_atencion between dateadd(dd,7,a.fecha_psico5) and dateadd(dd,30,a.fecha_psico5))	-- Entre cada consulta de salud mental o psicoterapia individual habrá un intervalo mínimo de 7 días y máximo de 30 días
and a.filtro='2. Dx de violencia hace 6 meses'
and b.id_cita in ( select distinct id_cita from #his_minsa
					where	(
						(cod_item in ('t740','t741','t742','t743','t748','t749') )	-- Dx de violencia.
						or ( substring(cod_item,1,3) between 'Y04' and 'Y08' )		-- Agresiones.
							) 	and b.tipo_dx in ('D','R') )
group by a.renaes_dx, a.renaes_apn, a.fecha_1atenapn, 
		a.num_doc, a.fecha_dx, a.fecha_dx_3m, a.fecha_dx_6m, filtro


--%%%%%%%%%%%%%%%%
--	3.INDICADOR
--%%%%%%%%%%%%%%%% 

-- 3.1 - Se procede a unir el denominador con los atributos del numerador para elaborar el reporte final e Insertar Información a Tabla Reporte. 
insert into #tabla_reporte
select 
  @año				año
, @mes_inicio		mes
, a.renaes_Dx
, a.renaes_apn
, a.fecha_1atenapn
, a.num_doc
, a.fecha_dx
, a.fecha_dx_3m
, a.fecha_dx_6m
, a.filtro
, b1.fecha_CSM1	,	isnull(b1.num_CSM1,0) num_CSM1
, b2.fecha_csm2	,	isnull(b2.num_CSM2,0) num_CSM2
, c1.fecha_psico1,	isnull(c1.num_psico1,0) num_psico1
, c2.fecha_psico2,	isnull(c2.num_psico2,0) num_psico2
, c3.fecha_psico3,	isnull(c3.num_psico3,0) num_psico3
, c4.fecha_psico4,	isnull(c4.num_psico4,0) num_psico4
, c5.fecha_psico5,	isnull(c5.num_psico5,0) num_psico5
, c6.fecha_psico6,	isnull(c6.num_psico6,0) num_psico6
, case when num_CSM1=1 and num_csm2=1 
   and num_psico1=1 and num_psico2=1 and num_psico3=1	then 1	else 0 end numerador_3m
, case 
when a.filtro='1. Dx de violencia hace 3 meses'
	 and num_CSM1=1 and num_csm2=1 
	 and num_psico1=1 and num_psico2=1 and num_psico3=1	then 1														--	 Si el dx fue identificado hace 03 meses previos al periodo de medición: minimo 02 CSM+ al menos 03 psicoterapia individual
when a.filtro='2. Dx de violencia hace 6 meses'
	 and num_CSM1=1 and num_csm2=1 
	 and num_psico1=1 and num_psico2=1 and num_psico3=1	and num_psico4=1 and num_psico5=1 and num_psico6=1 then 1	--	 Si el dx fue identificado hace 06 meses previos al periodo de medición: minimo 02 CSM+ al menos 06 psicoterapia individual.
else 0 end numerador
, denominador
from #denominador a
left join #num_CSM1			b1	on a.renaes_dx=b1.renaes_Dx and a.renaes_apn=b1.renaes_apn and a.fecha_1atenapn=b1.fecha_1atenapn and a.num_doc=b1.num_doc and a.fecha_dx=b1.fecha_dx
left join #num_CSM2			b2	on a.renaes_dx=b2.renaes_Dx and a.renaes_apn=b2.renaes_apn and a.fecha_1atenapn=b2.fecha_1atenapn and a.num_doc=b2.num_doc and a.fecha_dx=b2.fecha_dx
left join #num_psico1		c1	on a.renaes_dx=c1.renaes_Dx and a.renaes_apn=c1.renaes_apn and a.fecha_1atenapn=c1.fecha_1atenapn and a.num_doc=c1.num_doc and a.fecha_dx=c1.fecha_dx
left join #num_psico2		c2	on a.renaes_dx=c2.renaes_Dx and a.renaes_apn=c2.renaes_apn and a.fecha_1atenapn=c2.fecha_1atenapn and a.num_doc=c2.num_doc and a.fecha_dx=c2.fecha_dx
left join #num_psico3		c3	on a.renaes_dx=c3.renaes_Dx and a.renaes_apn=c3.renaes_apn and a.fecha_1atenapn=c3.fecha_1atenapn and a.num_doc=c3.num_doc and a.fecha_dx=c3.fecha_dx
left join #num_psico4		c4	on a.renaes_dx=c4.renaes_Dx and a.renaes_apn=c4.renaes_apn and a.fecha_1atenapn=c4.fecha_1atenapn and a.num_doc=c4.num_doc and a.fecha_dx=c4.fecha_dx
left join #num_psico5		c5	on a.renaes_dx=c5.renaes_Dx and a.renaes_apn=c5.renaes_apn and a.fecha_1atenapn=c5.fecha_1atenapn and a.num_doc=c5.num_doc and a.fecha_dx=c5.fecha_dx
left join #num_psico6		c6	on a.renaes_dx=c6.renaes_Dx and a.renaes_apn=c6.renaes_apn and a.fecha_1atenapn=c6.fecha_1atenapn and a.num_doc=c6.num_doc and a.fecha_dx=c6.fecha_dx


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
if object_id(N'FED25.TRAMAHIS_FED2024_25_Vii0101_Nominal', N'U') is not null drop table FED25.TRAMAHIS_FED2024_25_Vii0101_Nominal;
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
, a.renaes_Dx eess_renaes_Dx
, c.DESC_ESTAB eess_nombre
, isnull(b.fed,0) Distritos_FED_23_24
, isnull(b.fed_limametro,0) Distritos_FED_23_24_LimaMetro 
, isnull(b.Fed_2018,0) Distritos_FED_24_25
, isnull(b.fed_IC,0) Distritos_FED_IC
, a.*
into FED25.TRAMAHIS_FED2024_25_Vii0101_Nominal
from #tabla_reporte a
inner join #renaes c on converT(int,a.renaes_Dx)=convert(int,c.renaes)
inner join MaeUbigeo_20240808 b on convert(int,c.ubigeo)=convert(int,b.ubigeo)

-- Tabla: Reporte Consolidado
select periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, eess_cat, eess_renaes_Dx, eess_nombre, filtro
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
, sum(denominador)						denominador
, sum(numerador)						numerador
, sum(numerador_3m)						numerador_3m
, sum(num_CSM1)							num_CSM1
, sum(num_CSM2)							num_CSM2
, sum(num_Psico1)						num_Psico1
, sum(num_Psico2)						num_Psico2
, sum(num_Psico3)						num_Psico3
, sum(num_Psico4)						num_Psico4
, sum(num_Psico5)						num_Psico5
, sum(num_Psico6)						num_Psico6
from FED25.TRAMAHIS_FED2024_25_Vii0101_Nominal
group by periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, eess_cat, eess_renaes_Dx, eess_nombre, filtro
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
-------------------------------- =D 
