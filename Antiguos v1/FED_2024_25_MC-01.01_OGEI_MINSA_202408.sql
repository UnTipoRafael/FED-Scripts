/*=======================================================================================================================================
Compromiso Gestion: MC-01 | Mujeres con parto institucional procedentes de los distritos de quintiles 1 y 2 de pobreza, que 
							durante su gestación recibieron el paquete integrado de servicios.

Indicador	:	MC-01.01  | Porcentaje de mujeres con parto institucional, procedentes de los distritos de quintiles 1 y 2 de pobreza 
							, que durante su gestación recibieron el paquete integrado de servicios

Elaboración	:	MINISTERIO DESARROLLO E INCLUSION SOCIAL (MIDIS)
				DIRECCIÓN DE SEGUIMIENTO Y EVALUACIÓN (DGSE)
				DIRECCIÓN DE SEGUIMIENTO (DS)
				/ JHONATAN LAVI
Revision	:	MINISTERIO DE SALUD (MINSA)
				OFICINA GENERAL DE TECNOLOGIAS DE LA INFORMACION (OGTI)
				OFICINA DE GESTION DE LA INFORMACION (OGEI)
				Wilson URVIOLA ZAPATA - ANALISTA ESTADISTICO
Fecha		:	20/08/2024
============================================================================================================================================ */

-- Especificar la Base de datos a utilizar y en donde se guarda la información. 
use BDHIS_MINSA
go

/* **********************************************************
				Tablas.

En esta parte se procede a seleccionar las Tablas/tramas/Base de datos 
utilizadas para generar el indicador. Las Tablas/tramas/Base de datos utilizados 
para este indicador son:

- 1.HIS MINSA (Mes de evalucación)			| Cambia mes a mes.
- 2.HIS MINSA (Año Previo)
- 3.CNV (Mes de Evaluación)					| Cambia mes a mes.
- 4.RENAES (Mes de Evaluación)				| Cambia mes a mes.
************************************************************* */

-- 1.Tabla de datos : HIS - MINSA (Mes de evalucación)
if Object_id(N'tempdb..#his_minsa',N'U') is not null drop table #his_minsa;
select id_cita
	, renipress
	, convert(date,Fecha_Atencion) fecha_atencion
	, convert(int,Anio+Mes) aniomes
	, Id_Paciente
	, Tipo_Diagnostico tipo_dx
	, Codigo_Item  
	, valor_lab
into #his_minsa
from  [dbo].[NOMINAL_TRAMA_NUEVO] with (nolock)
where Codigo_Item  in (
					'85018','85018.01'														-- Dosaje de Hemoglobina
					,'86780','86592','86593','86318.01','86780.01'							-- Tamizaje de sífilis
					,'86703','86703.02','87389','86318.01','86703.01'						-- Tamizaje de VIH con prueba rápida
					,'81007','81002','81000.02'												-- Tamizaje de bacteriuria asintomática
					,'80055.01'																-- Perfil obstétrico
					,'Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z349','Z359'			-- Atencion Prenatal
					,'59401.04','99199.26'													-- suplemento  
					,'O990'																	-- Dx de Anemia 
				)
and sw=1					-- Validación del numero de documento.
and id_tipo_doc=1			-- Tipo de documento: DNI
and id_genero='F'			-- Sexo: Mujer


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
where cod_item in (
					'85018','85018.01'														-- Dosaje de Hemoglobina
					,'86780','86592','86593','86318.01','86780.01'							-- Tamizaje de sífilis
					,'86703','86703.02','87389','86318.01','86703.01'						-- Tamizaje de VIH con prueba rápida
					,'81007','81002','81000.02'												-- Tamizaje de bacteriuria asintomática
					,'80055.01'																-- Perfil obstétrico
					,'Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z349','Z359'			-- Atencion Prenatal
					,'59401.04','99199.26'													-- suplemento  
					,'O990'																	-- Dx de Anemia 
				)
and sw=1					-- Validación del numero de documento.
and id_tipo_doc=1			-- Tipo de documento: DNI
and id_genero='F'			-- Sexo: Mujer


-- 3.Tabla de datos : CNV (Mes de evaluación)
if Object_id(N'tempdb..#cnv',N'U') is not null drop table #cnv;
select distinct nu_doc_madre  num_doc,
				convert(date,fe_nacido) fecha_parto, 
				convert(int,dur_emb_parto) semana_nac,
				dpto_dom_madre + prov_dom_madre + dist_dom_madre ubigeo_reniec,
				converT(int,co_local) EESS_parto,
				ROW_NUMBER() OVER (order by (select null)) AS row_num
into #cnv
from BD_BACKUP_OGEI.dbo.TramaCNV with (nolock)
where convert(int,tip_doc_madre)=1		-- Tipo de documento: DNI
and sw_dni=1							-- Validación del numero de documento.
and convert(int,cont_dom_madre)=92		-- Partos de Gestantes Domiciliadas en Perú.
and convert(int,pais_dom_madre)=33		-- Partos de Gestantes Domiciliadas en Perú.
and convert(int,tip_lugar_nacido)=1	-- Partos realizados en Establecimientos de Salud. 


		--------------------------------------------
		--- Eliminar registros vacios o nulos ---
		delete from #cnv
		where num_doc is null or num_doc=''

		--------------------------------------------
		--- Eliminar registros Duplicados ---
		delete from #cnv
		where row_num  in
			(
				select distinct row_num
				from (
					select *
					, row_number() over (partition by num_doc order by fecha_parto desc, semana_nac desc) id		-- Se ordena la información con respecto al ultimo parto y las semanas de gestación mas larga.
					from #cnv
				) as t 
				where id<>1		-- Nos quedamos en la tabla con el ultimo parto de la gestante. 
			) 
		----------------------------------------

-- Tablade datos : Renaes
if Object_id(N'tempdb..#renaes',N'U') is not null drop table #renaes;
select *
into #renaes
from BD_BACKUP_OGEI.dbo.Renaes

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
if Object_id(N'tempdb..#reporte_final',N'U') is not null drop table #reporte_final;
create table #reporte_final 
(		
año							int,
mes							int,
num_doc						nvarchar(15),
fecha_parto					date,
semana_nac					int,
Gest_37sem					int,
ubigeo_reniec				nvarchar(6),
EESS_parto					nvarchar(150),
sector						nvarchar(50),
flag_indicador				int,
fecha_inicio_gestacion		date,
fecha_Ini_sem14				date,
fecha_Ini_sem28				date,
fecha_Ini_sem33				date,
fecha_Ini_sem37				date,
denominador					float,
numerador					float,
fecha_exam_hb				date,
num_exam_hb					int,
fecha_exam_sifilis			date,
num_exam_sifilis			int,
fecha_exam_VIH				date,
num_exam_VIH				int,
fecha_exam_bacteriuria		date,
num_exam_bacteriuria		int,
fecha_perfil_obstetrico		date,
num_perfil_obstetrico		int,
num_exam_aux				int,
fecha_APN1_1trim			date,
num_APN1_1trim				int,
fecha_APN1_2trim			date,
num_APN1_2trim				int,
fecha_APN2_2trim			date,
num_APN2_2trim				int,
fecha_APN1_3trim			date,
num_APN1_3trim				int,
fecha_APN2_3trim			date,
num_APN2_3trim				int,
fecha_APN3_3trim			date,
num_APN3_3trim				int,
num_APN						int,
fecha_entrega1_SfAf			date,
num_entrega1_SfAf			int,
fecha_entrega2_SfAf			date,
num_entrega2_SfAf			int,
fecha_entrega3_SfAf			date,
num_entrega3_SfAf			int,
fecha_entrega4_SfAf			date,
num_entrega4_SfAf			int,
fecha_entrega5_SfAf			date,
num_entrega5_SfAf			int,
num_entrega_SfAf			int
)
	
declare @mes_inicio int, 
		@mes_eval int, 
		@año int 

set @año=2024
set @mes_inicio=1 
set @mes_eval=9 --< Modificar segun mes de evaluación.

while @mes_inicio <= @mes_eval
begin

--%%%%%%%%%%%%%%%%
-- 1.DENOMINADOR
--%%%%%%%%%%%%%%%%

/* 1.1 - Mujeres con edad gestacional mayor o igual a 37 semanas de gestación, cuyo parto ha sido atendido en Establecimientos de Salud del MINSA */ 
if Object_id(N'tempdb..#cnv_den',N'U') is not null drop table #cnv_den;
select a.num_doc,
	   a.fecha_parto,
	   a.semana_nac,
	   iif(a.semana_nac>=37,1,0) Gest_37sem,
	   a.ubigeo_reniec,
	   b.desc_estab EESS_parto,
	   b.sector,
	   iif(semana_nac>=37 and convert(int,b.ambito)=1,1,0) flag_indicador		-- Edad gestacional mayor o igual a 37 semanas de gestación, cuyo parto ha sido atendido en Establecimientos de Salud del MINSA
into #cnv_den
from #cnv a
left join #renaes b on convert(int,a.EESS_parto)=convert(int,b.cod_estab)
where year(a.fecha_parto)=@año
and	month(a.fecha_parto)=@mes_inicio


/* 1.2 - Calculo de las semanas de gestación  */ 
if Object_id(N'tempdb..#DimFechaMin',N'U') is not null drop table #DimFechaMin;
select Año, Semana, min(Mes) min_mes
		, (select min(Dia) from DimFecha_MIDIS where semana=t1.semana and Año=t1.año and Mes=min(t1.mes)) min_dia
		,min(fecha) as fecha
into #DimFechaMin
from DimFecha_MIDIS t1
group by t1.Año, t1.Semana


/* 1.3 - Unión de Información para armar el denominador del indicador. 
-- 
Para evaluar las semanas de gestación, se considera la semana de gestación cumplida + 06 días.
 13 SG es igual a: 13 x 7 días + 6dias = 97dias. 
 27 SG es igual a: 27 x 7 días + 6dias = 195 días, 
 28 SG es igual a: 28 x 07 días+ 06 días = 202 días
 --
*/
if Object_id(N'tempdb..#denominador',N'U') is not null drop table #denominador;
select a.*
,convert(date,dateadd(day,(14*7),fecha_inicio_gestacion)) Fecha_Ini_sem14			-- Fecha Inicia Semana 14 de gestación.   
,convert(date,dateadd(day,(28*7),fecha_inicio_gestacion)) Fecha_Ini_sem28			-- Fecha Inicia Semana 28 de gestación.	
,convert(date,dateadd(day,(33*7),fecha_inicio_gestacion)) Fecha_Ini_sem33			-- Fecha Inicia Semana 33 de gestación.
,convert(date,dateadd(day,(37*7),fecha_inicio_gestacion)) Fecha_Ini_sem37			-- Fecha Inicia Semana 37 de gestación.
into #denominador
from(
		select a.*
		, convert(date,dfMin.fecha)  fecha_inicio_gestacion 
		from #cnv_den a
		join DIMFECHA_MIDIS df on DATEADD(dd,-(a.semana_nac*7),a.fecha_parto)=df.Fecha
		Join #DimFechaMin dfMin on df.Año = dfMin.Año and df.Semana = dfMin.Semana		
) a
JOIN DimFecha_MIDIS df1 on DATEADD(DAY,(14*7),a.fecha_inicio_gestacion )=  CONVERT(DATE,df1.Fecha)
JOIN #DimFechaMin dfMin1 on df1.Año = dfMin1.Año and df1.Semana = dfMin1.Semana


--%%%%%%%%%%%%%%%%
-- 2.NUMERADOR
--%%%%%%%%%%%%%%%%


--==========================================
--		EXAMENES AUXILIARES.
--==========================================

/* 2.1 Mujeres del denominador con Dosaje de Hemoglobina (85018 o 85018.01) con tipo de diagnóstico definitivo "D" en el primer trimestre (antes de las 14 semanas de gestación). */
if Object_id(N'tempdb..#num_exam_hb',N'U') is not null drop table #num_exam_hb;
select	a.num_doc,
		a.fecha_parto,
		a.semana_nac,
		a.ubigeo_reniec,
		min(b.fecha_atencion) fecha_exam_Hb,
		num_exam_Hb=1
into #num_exam_hb
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc	
where b.cod_item in ('85018','85018.01') and b.tipo_dx='D'													-- Dosaje de Hemoglobina
and b.id_cita in (	
					select distinct id_cita from #his_minsa		
					where cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z349','Z359')		-- Las prestaciones evaluadas deben registrar además algún código de control del embarazo o de la atención prenatal.
				  )
and ( b.fecha_atencion between a.fecha_inicio_gestacion and dateadd(dd,-1,a.Fecha_Ini_sem14) )				-- El primer trimestre (antes de las 14 semanas de gestación).
group by	a.num_doc, 
			a.fecha_parto, 
			a.semana_nac, 
			a.ubigeo_reniec

		
/* 2.2 Mujeres del denominador con Tamizaje de sífilis con prueba rápida (86780 o 86592 o 86593 o 86318.01 o 86780.01) con tipo de diagnóstico definitivo "D" en el primer trimestre (antes de las 14 semanas de gestación). */
if Object_id(N'tempdb..#num_exam_sifilis',N'U') is not null drop table #num_exam_sifilis;
select	a.num_doc,
		a.fecha_parto,
		a.semana_nac,
		a.ubigeo_reniec,
		min(b.fecha_atencion) fecha_exam_sifilis,
		num_exam_sifilis=1
into #num_exam_sifilis
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc	
where b.cod_item in ('86780','86592','86593','86318.01','86780.01') and b.tipo_dx='D'					-- Tamizaje de sífilis
and b.id_cita in (	
					select distinct id_cita from #his_minsa		
					where cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z349','Z359')	-- Las prestaciones evaluadas deben registrar además algún código de control del embarazo o de la atención prenatal.
				  )
and ( b.fecha_atencion between a.fecha_inicio_gestacion and dateadd(dd,-1,a.Fecha_Ini_sem14) )			-- El primer trimestre (antes de las 14 semanas de gestación).
group by	a.num_doc, 
			a.fecha_parto, 
			a.semana_nac, 
			a.ubigeo_reniec

		
/* 2.3 Mujeres del denominador con Tamizaje de VIH con prueba rápida (86703 o 86703.02 o 87389 o 86318.01 o 86703.01) con tipo de diagnóstico definitivo "D" en el primer trimestre (antes de las 14 semanas de gestación). */
if Object_id(N'tempdb..#num_exam_VIH',N'U') is not null drop table #num_exam_VIH;
select	a.num_doc,
		a.fecha_parto,
		a.semana_nac,
		a.ubigeo_reniec,
		min(b.fecha_atencion) fecha_exam_VIH,
		num_exam_VIH=1
into #num_exam_VIH
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc	
where b.cod_item in ('86703','86703.02','87389','86318.01','86703.01') and b.tipo_dx='D'				-- Tamizaje de VIH con prueba rápida
and b.id_cita in (	
					select distinct id_cita from #his_minsa		
					where cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z349','Z359')	-- Las prestaciones evaluadas deben registrar además algún código de control del embarazo o de la atención prenatal.
				  )
and ( b.fecha_atencion between a.fecha_inicio_gestacion and dateadd(dd,-1,a.Fecha_Ini_sem14) )			-- El primer trimestre (antes de las 14 semanas de gestación).
group by	a.num_doc, 
			a.fecha_parto, 
			a.semana_nac, 
			a.ubigeo_reniec

		
/* 2.4 Mujeres del denominador con Tamizaje de bacteriuria asintomática (81007 o 81002 o 81000.02) con tipo de diagnóstico definitivo "D" en el primer trimestre (antes de las 14 semanas de gestación). */
if Object_id(N'tempdb..#num_exam_bacteriuria',N'U') is not null drop table #num_exam_bacteriuria;
select	a.num_doc,
		a.fecha_parto,
		a.semana_nac,
		a.ubigeo_reniec,
		min(b.fecha_atencion) fecha_exam_bacteriuria,
		num_exam_bacteriuria=1
into #num_exam_bacteriuria
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc	
where b.cod_item in ('81007','81002','81000.02') and b.tipo_dx='D'										-- Tamizaje de bacteriuria asintomática
and b.id_cita in (	
					select distinct id_cita from #his_minsa		
					where cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z349','Z359')	-- Las prestaciones evaluadas deben registrar además algún código de control del embarazo o de la atención prenatal.
				  )
and ( b.fecha_atencion between a.fecha_inicio_gestacion and dateadd(dd,-1,a.Fecha_Ini_sem14) )			-- El primer trimestre (antes de las 14 semanas de gestación).
group by	a.num_doc, 
			a.fecha_parto, 
			a.semana_nac, 
			a.ubigeo_reniec

		
/* 2.5 Mujeres del denominador con Perfil obstétrico (80055.01) con tipo de diagnóstico definitivo "D" en el primer trimestre (antes de las 14 semanas de gestación). */
if Object_id(N'tempdb..#num_perfil_obstetrico',N'U') is not null drop table #num_perfil_obstetrico;
select	a.num_doc,
		a.fecha_parto,
		a.semana_nac,
		a.ubigeo_reniec,
		min(b.fecha_atencion) fecha_perfil_obstetrico,
		num_perfil_obstetrico=1
into #num_perfil_obstetrico
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc	
where b.cod_item='80055.01' and b.tipo_dx='D'															-- Perfil obstétrico
and b.id_cita in (	
					select distinct id_cita from #his_minsa		
					where cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z349','Z359')	-- Las prestaciones evaluadas deben registrar además algún código de control del embarazo o de la atención prenatal.
				  )
and ( b.fecha_atencion between a.fecha_inicio_gestacion and dateadd(dd,-1,a.Fecha_Ini_sem14) )			-- El primer trimestre (antes de las 14 semanas de gestación).
group by	a.num_doc, 
			a.fecha_parto, 
			a.semana_nac, 
			a.ubigeo_reniec
			

/* 2.6 Mujeres del denominador que cumplen con los siguientes criterios: 
	 Haber recibido al menos una vez, en el primer trimestre (antes de las 14 semanas de gestación), los siguientes exámenes auxiliares:
		- Dosaje de Hemoglobina, Tamizaje de sífilis,Tamizaje de VIH con prueba rápida,Tamizaje de bacteriuria asintomática 
			o
		- Perfil obstétrico */
if Object_id(N'tempdb..#num_exam_aux',N'U') is not null drop table #num_exam_aux;
select	a.num_doc,
		a.fecha_parto,
		a.semana_nac,
		a.ubigeo_reniec,
		b1.fecha_exam_Hb,			isnull(b1.num_exam_Hb,0)				num_exam_Hb,
		b2.fecha_exam_sifilis,		isnull(b2.num_exam_sifilis,0)			num_exam_sifilis,
		b3.fecha_exam_VIH,			isnull(b3.num_exam_VIH,0)				num_exam_VIH,
		b4.fecha_exam_bacteriuria,	isnull(b4.num_exam_bacteriuria,0)		num_exam_bacteriuria,
		b5.fecha_perfil_obstetrico,	isnull(b5.num_perfil_obstetrico,0)		num_perfil_obstetrico,
		iif(   (b1.num_exam_Hb=1 and b2.num_exam_sifilis=1 and b3.num_exam_VIH=1 and b4.num_exam_bacteriuria=1) or (b5.num_perfil_obstetrico=1),1,0) num_exam_aux			--  Haber recibido al menos una vez 4 examenes o Perfil obstetrico
into #num_exam_aux
from #denominador a
left join #num_exam_hb				b1	on	a.num_doc=b1.num_doc and a.ubigeo_reniec=b1.ubigeo_reniec and a.fecha_parto=b1.fecha_parto and a.semana_nac=b1.semana_nac
left join #num_exam_sifilis			b2	on	a.num_doc=b2.num_doc and a.ubigeo_reniec=b2.ubigeo_reniec and a.fecha_parto=b2.fecha_parto and a.semana_nac=b2.semana_nac	
left join #num_exam_VIH				b3	on	a.num_doc=b3.num_doc and a.ubigeo_reniec=b3.ubigeo_reniec and a.fecha_parto=b3.fecha_parto and a.semana_nac=b3.semana_nac	
left join #num_exam_bacteriuria		b4	on	a.num_doc=b4.num_doc and a.ubigeo_reniec=b4.ubigeo_reniec and a.fecha_parto=b4.fecha_parto and a.semana_nac=b4.semana_nac	
left join #num_perfil_obstetrico	b5	on	a.num_doc=b5.num_doc and a.ubigeo_reniec=b5.ubigeo_reniec and a.fecha_parto=b5.fecha_parto and a.semana_nac=b5.semana_nac	


--==========================================
--		ATENCION PRENATAL
--==========================================


/* 2.7 Mujeres del denominador con Al menos 01 APN [Menor a 14 SG] 
	-- Del inicio de la gestación hasta las 32 SG el intervalo mínimo entre cada atención prenatal es de 28 días. */
if Object_id(N'tempdb..#num_APN1_1Trim',N'U') is not null drop table #num_APN1_1Trim;
select distinct	a.num_doc,
				a.fecha_parto,
				a.semana_nac,
				a.ubigeo_reniec,
				a.fecha_inicio_gestacion,
				a.Fecha_Ini_sem14,
				a.Fecha_Ini_sem28,
				a.Fecha_Ini_sem33,
				a.Fecha_Ini_sem37,
				b.fecha_atencion fecha_APN1_1Trim,
				num_APN1_1Trim=1
into #num_APN1_1Trim
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc	
where b.cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593')									-- Atencion Prenatal
and ( b.fecha_atencion between a.fecha_inicio_gestacion and dateadd(dd,-1,a.Fecha_Ini_sem14) )			-- El primer trimestre (antes de las 14 semanas de gestación).


/* 2.8 Mujeres del denominador con Al menos 02 APN [De 14 a menos de 28 SG].
	-- Primer APN. 
	-- Del inicio de la gestación hasta las 32 SG el intervalo mínimo entre cada atención prenatal es de 28 días. */
if Object_id(N'tempdb..#num_APN1_2Trim',N'U') is not null drop table #num_APN1_2Trim;
select distinct	a.num_doc,
				a.fecha_parto,
				a.semana_nac,
				a.ubigeo_reniec,
				a.fecha_inicio_gestacion,
				a.Fecha_Ini_sem14,
				a.Fecha_Ini_sem28,
				a.Fecha_Ini_sem33,
				a.Fecha_Ini_sem37,
				b.fecha_atencion fecha_APN1_2Trim,
				num_APN1_2Trim=1
into #num_APN1_2Trim
from #num_APN1_1Trim a
inner join #his_minsa b on a.num_doc=b.num_doc	
where b.cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593')									-- Atencion Prenatal
and ( b.fecha_atencion between a.Fecha_Ini_sem14 and dateadd(dd,-1,a.Fecha_Ini_sem28) )					-- El Segundo trimestre (De 14 a menos de 28 SG).
and b.fecha_atencion>=dateadd(dd,28,a.fecha_APN1_1Trim)													-- Intervalo mínimo entre cada atención prenatal es de 28 días [hasta las 32 SG].


/* 2.9 Mujeres del denominador con Al menos 02 APN [De 14 a menos de 28 SG].
	-- Segundo APN. 
	-- Del inicio de la gestación hasta las 32 SG el intervalo mínimo entre cada atención prenatal es de 28 días. */
if Object_id(N'tempdb..#num_APN2_2Trim',N'U') is not null drop table #num_APN2_2Trim;
select distinct		a.num_doc,
					a.fecha_parto,
					a.semana_nac,
					a.ubigeo_reniec,
					a.fecha_inicio_gestacion,
					a.Fecha_Ini_sem14,
					a.Fecha_Ini_sem28,
					a.Fecha_Ini_sem33,
					a.Fecha_Ini_sem37,
					b.fecha_atencion fecha_APN2_2Trim,
					num_APN2_2Trim=1
into #num_APN2_2Trim
from #num_APN1_2Trim a
inner join #his_minsa b on a.num_doc=b.num_doc	
where b.cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593')									-- Atencion Prenatal
and ( b.fecha_atencion between a.Fecha_Ini_sem14 and dateadd(dd,-1,a.Fecha_Ini_sem28) )					-- El Segundo trimestre (De 14 a menos de 28 SG).
and b.fecha_atencion>=dateadd(dd,28,a.fecha_APN1_2Trim)													-- Intervalo mínimo entre cada atención prenatal es de 28 días [hasta las 32 SG].


/* 2.10 Mujeres del denominador con Al menos 03 APN [De 28 SG hasta el termino del embarazo].
	-- Primer APN. 
	-- Del inicio de la gestación hasta las 32 SG el intervalo mínimo entre cada atención prenatal es de 28 días.
	-- A partir de las 33 SG hasta las 36 SG el intervalo mínimo entre cada atención prenatal es de 13 días. 
	-- A partir de las 37 SG, hasta culminar la gestación, el intervalo mínimo es de 07 días
*/
if Object_id(N'tempdb..#num_APN1_3Trim',N'U') is not null drop table #num_APN1_3Trim;
select distinct	a.num_doc,
				a.fecha_parto,
				a.semana_nac,
				a.ubigeo_reniec,
				a.fecha_inicio_gestacion,
				a.Fecha_Ini_sem14,
				a.Fecha_Ini_sem28,
				a.Fecha_Ini_sem33,
				a.Fecha_Ini_sem37,
				b.fecha_atencion fecha_APN1_3Trim,
				num_APN1_3Trim=1
into #num_APN1_3Trim
from #num_APN2_2Trim a
inner join #his_minsa b on a.num_doc=b.num_doc	
where b.cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593')									-- Atencion Prenatal
and ( b.fecha_atencion between a.Fecha_Ini_sem28 and a.fecha_parto )									-- El Tercer trimestre  [De 28 SG hasta el termino del embarazo].
and b.fecha_atencion>=dateadd(dd,28,a.fecha_APN2_2Trim)													-- Intervalo mínimo entre cada atención prenatal es de 28 días [hasta las 32 SG].


/* 2.11 Mujeres del denominador con Al menos 03 APN [De 28 SG hasta el termino del embarazo].
	-- Segundo APN. 
	-- Del inicio de la gestación hasta las 32 SG el intervalo mínimo entre cada atención prenatal es de 28 días.
	-- A partir de las 33 SG hasta las 36 SG el intervalo mínimo entre cada atención prenatal es de 13 días. 
	-- A partir de las 37 SG, hasta culminar la gestación, el intervalo mínimo es de 07 días.
*/
if Object_id(N'tempdb..#num_APN2_3Trim',N'U') is not null drop table #num_APN2_3Trim;
select distinct 	num_doc,
					fecha_parto,
					semana_nac,
					ubigeo_reniec,
					fecha_inicio_gestacion,
					Fecha_Ini_sem14,
					Fecha_Ini_sem28,
					Fecha_Ini_sem33,
					Fecha_Ini_sem37,
					iif(fecha_apn2_3trim_28s is not null, fecha_apn2_3trim_28s
										, iif(fecha_apn2_3trim_33s is not null,fecha_apn2_3trim_33s
												, iif(fecha_apn2_3trim_37s is not null,fecha_apn2_3trim_37s,null))) fecha_apn2_3trim,
					num_apn2_3trim=1
into #num_APN2_3Trim
from (
		select	a.*,
				iif( (b.fecha_atencion between a.Fecha_Ini_sem28 and dateadd(dd,-1,a.Fecha_Ini_sem33)) and b.fecha_atencion>=dateadd(dd,28,fecha_APN1_3Trim), b.fecha_atencion, null) fecha_apn2_3trim_28s,			-- Inicio de la gestación hasta las 32 SG el intervalo mínimo entre cada atención prenatal es de 28 días
				iif( (b.fecha_atencion between a.Fecha_Ini_sem33 and dateadd(dd,-1,a.Fecha_Ini_sem37)) and b.fecha_atencion>=dateadd(dd,13,fecha_APN1_3Trim), b.fecha_atencion, null) fecha_apn2_3trim_33s,			-- A partir de las 33 SG hasta las 36 SG el intervalo mínimo entre cada atención prenatal es de 13 días
				iif( (b.fecha_atencion between a.Fecha_Ini_sem37 and a.fecha_parto) and b.fecha_atencion>=dateadd(dd,7,fecha_APN1_3Trim), b.fecha_atencion, null) fecha_apn2_3trim_37s								-- A partir de las 37 SG, hasta culminar la gestación, el intervalo mínimo es de 07 días				

		from #num_APN1_3Trim a
		inner join #his_minsa b on a.num_doc=b.num_doc	
		where b.cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593')									-- Atencion Prenatal
		and ( b.fecha_atencion between a.Fecha_Ini_sem28 and a.fecha_parto )									-- El Tercer trimestre  [De 28 SG hasta el termino del embarazo].
) as t
where fecha_apn2_3trim_28s is not null or fecha_apn2_3trim_33s is not null or fecha_apn2_3trim_37s is not null


/* 2.12 Mujeres del denominador con Al menos 03 APN [De 28 SG hasta el termino del embarazo].
	-- Tercer APN. 
	-- Del inicio de la gestación hasta las 32 SG el intervalo mínimo entre cada atención prenatal es de 28 días.
	-- A partir de las 33 SG hasta las 36 SG el intervalo mínimo entre cada atención prenatal es de 13 días. 
	-- A partir de las 37 SG, hasta culminar la gestación, el intervalo mínimo es de 07 días.
*/
if Object_id(N'tempdb..#num_APN3_3Trim',N'U') is not null drop table #num_APN3_3Trim;
select distinct	num_doc,
				fecha_parto,
				semana_nac,
				ubigeo_reniec,
				fecha_inicio_gestacion,
				Fecha_Ini_sem14,
				Fecha_Ini_sem28,
				Fecha_Ini_sem33,
				Fecha_Ini_sem37,
				iif(fecha_apn3_3trim_28s is not null, fecha_apn3_3trim_28s
							, iif(fecha_apn3_3trim_33s is not null,fecha_apn3_3trim_33s
									, iif(fecha_apn3_3trim_37s is not null,fecha_apn3_3trim_37s,null))) fecha_apn3_3trim,
				num_apn3_3trim=1
into #num_APN3_3Trim
from (
		select	a.*,
				iif( (b.fecha_atencion between a.Fecha_Ini_sem28 and dateadd(dd,-1,a.Fecha_Ini_sem33)) and b.fecha_atencion>=dateadd(dd,28,fecha_APN2_3Trim), b.fecha_atencion, null) fecha_apn3_3trim_28s,			-- Inicio de la gestación hasta las 32 SG el intervalo mínimo entre cada atención prenatal es de 28 días
				iif( (b.fecha_atencion between a.Fecha_Ini_sem33 and dateadd(dd,-1,a.Fecha_Ini_sem37)) and b.fecha_atencion>=dateadd(dd,13,fecha_APN2_3Trim), b.fecha_atencion, null) fecha_apn3_3trim_33s,			-- A partir de las 33 SG hasta las 36 SG el intervalo mínimo entre cada atención prenatal es de 13 días
				iif( (b.fecha_atencion between a.Fecha_Ini_sem37 and a.fecha_parto) and b.fecha_atencion>=dateadd(dd,7,fecha_APN2_3Trim), b.fecha_atencion, null) fecha_apn3_3trim_37s								-- A partir de las 37 SG, hasta culminar la gestación, el intervalo mínimo es de 07 días				

		from #num_APN2_3Trim a
		inner join #his_minsa b on a.num_doc=b.num_doc	
		where b.cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593')									-- Atencion Prenatal
		and ( b.fecha_atencion between a.Fecha_Ini_sem28 and a.fecha_parto )									-- El Tercer trimestre  [De 28 SG hasta el termino del embarazo].
) as t
where fecha_apn3_3trim_28s is not null or fecha_apn3_3trim_33s is not null or fecha_apn3_3trim_37s is not null
			
			--  Haber recibido al menos 6 Atenciones Prenatales
/* 2.13 Mujeres del denominador que Han recibido al menos seis (06) atenciones prenatales, en el transcurso de la gestación */
if Object_id(N'tempdb..#num_apn',N'U') is not null drop table #num_apn;
select	a.num_doc,
		a.fecha_parto,
		a.semana_nac,
		a.ubigeo_reniec,
		min(b1.fecha_APN1_1Trim) fecha_APN1_1Trim,	max(isnull(b1.num_APN1_1Trim,0))	num_APN1_1Trim,
		min(b2.fecha_APN1_2Trim) fecha_APN1_2Trim,	max(isnull(b2.num_APN1_2Trim,0))	num_APN1_2Trim,
		min(b3.fecha_APN2_2Trim) fecha_APN2_2Trim,	max(isnull(b3.num_APN2_2Trim,0))	num_APN2_2Trim,
		min(b4.fecha_APN1_3Trim) fecha_APN1_3Trim,	max(isnull(b4.num_APN1_3Trim,0))	num_APN1_3Trim,
		min(b5.fecha_APN2_3Trim) fecha_APN2_3Trim,	max(isnull(b5.num_APN2_3Trim,0))	num_APN2_3Trim,
		min(b6.fecha_APN3_3Trim) fecha_APN3_3Trim,	max(isnull(b6.num_APN3_3Trim,0))	num_APN3_3Trim,
		max(iif(b6.num_APN3_3Trim=1,1,0))												num_APN				--	Han recibido al menos seis (06) atenciones prenatales
into #num_apn
from #denominador a
left join #num_APN1_1Trim			b1	on	a.num_doc=b1.num_doc and a.ubigeo_reniec=b1.ubigeo_reniec and a.fecha_parto=b1.fecha_parto and a.semana_nac=b1.semana_nac
left join #num_APN1_2Trim			b2	on	a.num_doc=b2.num_doc and a.ubigeo_reniec=b2.ubigeo_reniec and a.fecha_parto=b2.fecha_parto and a.semana_nac=b2.semana_nac	
left join #num_APN2_2Trim			b3	on	a.num_doc=b3.num_doc and a.ubigeo_reniec=b3.ubigeo_reniec and a.fecha_parto=b3.fecha_parto and a.semana_nac=b3.semana_nac	
left join #num_APN1_3Trim			b4	on	a.num_doc=b4.num_doc and a.ubigeo_reniec=b4.ubigeo_reniec and a.fecha_parto=b4.fecha_parto and a.semana_nac=b4.semana_nac	
left join #num_APN2_3Trim			b5	on	a.num_doc=b5.num_doc and a.ubigeo_reniec=b5.ubigeo_reniec and a.fecha_parto=b5.fecha_parto and a.semana_nac=b5.semana_nac	
left join #num_APN3_3Trim			b6	on	a.num_doc=b6.num_doc and a.ubigeo_reniec=b6.ubigeo_reniec and a.fecha_parto=b6.fecha_parto and a.semana_nac=b6.semana_nac	
group by	a.num_doc, 
			a.fecha_parto, 
			a.semana_nac, 
			a.ubigeo_reniec


--==============================================
--	ENTREGA SULFATO FERROSO + ACIDO FOLICO
--==============================================


/* 2.14 Haber recibido en el transcurso de la gestación, al menos cinco (05) entregas de sulfato ferroso + ácido fólico (59401.04 o 99199.26 o O990+ 59401.04 o O990 + 99199.26) , con un intervalo mensual.
		-- Primera Entrega Sulfato Ferroso + Acido Folico. 
		-- Para efectos de búsqueda de datos se considera un intervalo mínimo de 28 y 32 días.  */
if Object_id(N'tempdb..#num_Entrega1_SfAf',N'U') is not null drop table #num_Entrega1_SfAf;
select distinct a.num_doc,
				a.fecha_parto,
				a.semana_nac,
				a.ubigeo_reniec,
				a.fecha_inicio_gestacion,
				b.fecha_atencion Fecha_Entrega1_SfAF,
				num_Entrega1_SfAf=1
into #num_Entrega1_SfAf
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc		
where 
( ( b.cod_item in ('59401.04','99199.26')	)																						-- Entregas de sulfato ferroso + ácido fólico
or	
  ( b.cod_item in ('59401.04','99199.26') and id_cita in (select distinct id_cita from #his_minsa where cod_item='O990') )			-- Entregas de sulfato ferroso + ácido fólico (Dx Anemia)
)													
and b.id_cita in (	
					select distinct id_cita from #his_minsa		
					where cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z349','Z359')	-- Las prestaciones evaluadas deben registrar además algún código de control del embarazo o de la atención prenatal.
				  )
and ( b.fecha_atencion between a.fecha_inicio_gestacion and a.fecha_parto )	


/* 2.15 Haber recibido en el transcurso de la gestación, al menos cinco (05) entregas de sulfato ferroso + ácido fólico (59401.04 o 99199.26 o O990+ 59401.04 o O990 + 99199.26) , con un intervalo mensual.
		-- Segunda Entrega Sulfato Ferroso + Acido Folico. 
		-- Para efectos de búsqueda de datos se considera un intervalo mínimo de 28 y 32 días.  */
if Object_id(N'tempdb..#num_Entrega2_SfAf',N'U') is not null drop table #num_Entrega2_SfAf;
select distinct a.num_doc,
				a.fecha_parto,
				a.semana_nac,
				a.ubigeo_reniec,
				a.fecha_inicio_gestacion,
				b.fecha_atencion Fecha_Entrega2_SfAF,
				num_Entrega2_SfAf=1
into #num_Entrega2_SfAf
from #num_Entrega1_SfAf a
inner join #his_minsa b on a.num_doc=b.num_doc		
where 
( ( b.cod_item in ('59401.04','99199.26')	)																						-- Entregas de sulfato ferroso + ácido fólico
or	
  ( b.cod_item in ('59401.04','99199.26') and id_cita in (select distinct id_cita from #his_minsa where cod_item='O990') )			-- Entregas de sulfato ferroso + ácido fólico (Dx Anemia)
)													
and b.id_cita in (	
					select distinct id_cita from #his_minsa		
					where cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z349','Z359')								-- Las prestaciones evaluadas deben registrar además algún código de control del embarazo o de la atención prenatal.
				  )
and ( b.fecha_atencion between a.fecha_inicio_gestacion and a.fecha_parto )	
and (b.fecha_atencion >= dateadd(dd,28,a.Fecha_Entrega1_SfAF) )						-- Para efectos de búsqueda de datos se considera un intervalo mínimo de 28 y 32 días


/* 2.16 Haber recibido en el transcurso de la gestación, al menos cinco (05) entregas de sulfato ferroso + ácido fólico (59401.04 o 99199.26 o O990+ 59401.04 o O990 + 99199.26) , con un intervalo mensual.
		-- Tercera Entrega Sulfato Ferroso + Acido Folico. 
		-- Para efectos de búsqueda de datos se considera un intervalo mínimo de 28 y 32 días.  */
if Object_id(N'tempdb..#num_entrega3_SfAf',N'U') is not null drop table #num_entrega3_SfAf;
select distinct a.num_doc,
				a.fecha_parto,
				a.semana_nac,
				a.ubigeo_reniec,
				a.fecha_inicio_gestacion,
				b.fecha_atencion Fecha_entrega3_SfAF,
				num_entrega3_SfAf=1
into #num_entrega3_SfAf
from #num_entrega2_SfAf a
inner join #his_minsa b on a.num_doc=b.num_doc		
where 
( ( b.cod_item in ('59401.04','99199.26')	)																						-- Entregas de sulfato ferroso + ácido fólico
or	
  ( b.cod_item in ('59401.04','99199.26') and id_cita in (select distinct id_cita from #his_minsa where cod_item='O990') )			-- Entregas de sulfato ferroso + ácido fólico (Dx Anemia)
)													
and b.id_cita in (	
					select distinct id_cita from #his_minsa		
					where cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z349','Z359')								-- Las prestaciones evaluadas deben registrar además algún código de control del embarazo o de la atención prenatal.
				  )
and ( b.fecha_atencion between a.fecha_inicio_gestacion and a.fecha_parto )	
and ( b.fecha_atencion >= dateadd(dd,28,a.Fecha_entrega2_SfAF)  )						-- Para efectos de búsqueda de datos se considera un intervalo mayor o igual a 28 dias entre cada entrega


/* 2.17 Haber recibido en el transcurso de la gestación, al menos cinco (05) entregas de sulfato ferroso + ácido fólico (59401.04 o 99199.26 o O990+ 59401.04 o O990 + 99199.26) , con un intervalo mensual.
		-- Cuarto Entrega Sulfato Ferroso + Acido Folico. 
		-- Para efectos de búsqueda de datos se considera un intervalo mínimo de 28 y 32 días.  */
if Object_id(N'tempdb..#num_entrega4_SfAf',N'U') is not null drop table #num_entrega4_SfAf;
select distinct a.num_doc,
				a.fecha_parto,
				a.semana_nac,
				a.ubigeo_reniec,
				a.fecha_inicio_gestacion,
				b.fecha_atencion Fecha_entrega4_SfAF,
				num_entrega4_SfAf=1
into #num_entrega4_SfAf
from #num_entrega3_SfAf a
inner join #his_minsa b on a.num_doc=b.num_doc		
where 
( ( b.cod_item in ('59401.04','99199.26')	)																						-- Entregas de sulfato ferroso + ácido fólico
or	
  ( b.cod_item in ('59401.04','99199.26') and id_cita in (select distinct id_cita from #his_minsa where cod_item='O990') )			-- Entregas de sulfato ferroso + ácido fólico (Dx Anemia)
)													
and b.id_cita in (	
					select distinct id_cita from #his_minsa		
					where cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z349','Z359')								-- Las prestaciones evaluadas deben registrar además algún código de control del embarazo o de la atención prenatal.
				  )
and ( b.fecha_atencion between a.fecha_inicio_gestacion and a.fecha_parto )	
and ( b.fecha_atencion >= dateadd(dd,28,a.Fecha_entrega3_SfAF)  )						-- Para efectos de búsqueda de datos se considera un intervalo mínimo de 28 y 32 días


/* 2.18 Haber recibido en el transcurso de la gestación, al menos cinco (05) entregas de sulfato ferroso + ácido fólico (59401.04 o 99199.26 o O990+ 59401.04 o O990 + 99199.26) , con un intervalo mensual.
		-- Quinto Entrega Sulfato Ferroso + Acido Folico. 
		-- Para efectos de búsqueda de datos se considera un intervalo mínimo de 28 y 32 días.  */
if Object_id(N'tempdb..#num_entrega5_SfAf',N'U') is not null drop table #num_entrega5_SfAf;
select distinct a.num_doc,
				a.fecha_parto,
				a.semana_nac,
				a.ubigeo_reniec,
				a.fecha_inicio_gestacion,
				b.fecha_atencion Fecha_entrega5_SfAF,
				num_entrega5_SfAf=1
into #num_entrega5_SfAf
from #num_entrega4_SfAf a
inner join #his_minsa b on a.num_doc=b.num_doc		
where 
( ( b.cod_item in ('59401.04','99199.26')	)																						-- Entregas de sulfato ferroso + ácido fólico
or	
  ( b.cod_item in ('59401.04','99199.26') and id_cita in (select distinct id_cita from #his_minsa where cod_item='O990') )			-- Entregas de sulfato ferroso + ácido fólico (Dx Anemia)
)													
and b.id_cita in (	
					select distinct id_cita from #his_minsa		
					where cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z349','Z359')								-- Las prestaciones evaluadas deben registrar además algún código de control del embarazo o de la atención prenatal.
				  )
and ( b.fecha_atencion between a.fecha_inicio_gestacion and a.fecha_parto )	
and ( b.fecha_atencion >= dateadd(dd,28,a.Fecha_entrega4_SfAF) )						-- Para efectos de búsqueda de datos se considera un intervalo mínimo de 28 y 32 días


/* 2.19 Gestantes que han recibido en el transcurso de la gestación, al menos cinco (05) entregas de sulfato ferroso + ácido fólico */
if Object_id(N'tempdb..#num_entrega_SfAf',N'U') is not null drop table #num_entrega_SfAf;
select	a.num_doc,
		a.fecha_parto,
		a.semana_nac,
		a.ubigeo_reniec,
		min(b1.Fecha_Entrega1_SfAF)	Fecha_Entrega1_SfAF,	max(isnull(b1.num_Entrega1_SfAf,0))	num_Entrega1_SfAf, 
		min(b2.Fecha_Entrega2_SfAF)	Fecha_Entrega2_SfAF,	max(isnull(b2.num_Entrega2_SfAf,0))	num_Entrega2_SfAf,
		min(b3.Fecha_entrega3_SfAF)	Fecha_Entrega3_SfAF,	max(isnull(b3.num_entrega3_SfAf,0))	num_Entrega3_SfAf,
		min(b4.Fecha_entrega4_SfAF)	Fecha_Entrega4_SfAF,	max(isnull(b4.num_entrega4_SfAf,0))	num_Entrega4_SfAf,
		min(b5.Fecha_entrega5_SfAF)	Fecha_Entrega5_SfAF,	max(isnull(b5.num_entrega5_SfAf,0)) num_Entrega5_SfAf,
		max(iif(b5.num_entrega5_SfAf=1,1,0)) num_entrega_SfAf
into #num_entrega_SfAf
from #denominador a
left join #num_Entrega1_SfAf	b1	on	a.num_doc=b1.num_doc and a.ubigeo_reniec=b1.ubigeo_reniec and a.fecha_parto=b1.fecha_parto and a.semana_nac=b1.semana_nac
left join #num_Entrega2_SfAf	b2	on	a.num_doc=b2.num_doc and a.ubigeo_reniec=b2.ubigeo_reniec and a.fecha_parto=b2.fecha_parto and a.semana_nac=b2.semana_nac	
left join #num_entrega3_SfAf	b3	on	a.num_doc=b3.num_doc and a.ubigeo_reniec=b3.ubigeo_reniec and a.fecha_parto=b3.fecha_parto and a.semana_nac=b3.semana_nac	
left join #num_entrega4_SfAf	b4	on	a.num_doc=b4.num_doc and a.ubigeo_reniec=b4.ubigeo_reniec and a.fecha_parto=b4.fecha_parto and a.semana_nac=b4.semana_nac	
left join #num_entrega5_SfAf	b5	on	a.num_doc=b5.num_doc and a.ubigeo_reniec=b5.ubigeo_reniec and a.fecha_parto=b5.fecha_parto and a.semana_nac=b5.semana_nac	
group by	a.num_doc, 
			a.fecha_parto, 
			a.semana_nac, 
			a.ubigeo_reniec


--%%%%%%%%%%%%%%%%
--	3.INDICADOR
--%%%%%%%%%%%%%%%% 

/* 3.1 - Se procede a unir el denominador con los atributos del numerador para elaborar el reporte final
	   - Insertar Información a Tabla Reporte */
insert into #reporte_final
select	@año año,
		@mes_inicio mes,	
		a.num_doc,
		a.fecha_parto,
		a.semana_nac,
		a.Gest_37sem,
		a.ubigeo_reniec,
		a.EESS_parto,
		a.sector,
		a.flag_indicador,
		a.fecha_inicio_gestacion,
		a.Fecha_Ini_sem14,
		a.Fecha_Ini_sem28,
		a.Fecha_Ini_sem33,
		a.Fecha_Ini_sem37,
		denominador=1,
		iif(b1.num_exam_aux=1 and b2.num_apn=1 and b3.num_entrega_SfAf=1,1,0) numerador,
		-- Examenes auxiliares
		b1.fecha_exam_Hb,			b1.num_exam_Hb,
		b1.fecha_exam_sifilis,		b1.num_exam_sifilis,
		b1.fecha_exam_VIH,			b1.num_exam_VIH,
		b1.fecha_exam_bacteriuria,	b1.num_exam_bacteriuria,
		b1.fecha_perfil_obstetrico,	b1.num_perfil_obstetrico,
		b1.num_exam_aux,
		-- Apn
		b2.fecha_APN1_1Trim,		b2.num_APN1_1Trim,
		b2.fecha_APN1_2Trim,		b2.num_APN1_2Trim,
		b2.fecha_APN2_2Trim,		b2.num_APN2_2Trim,
		b2.fecha_APN1_3Trim,		b2.num_APN1_3Trim,
		b2.fecha_APN2_3Trim,		b2.num_APN2_3Trim,
		b2.fecha_APN3_3Trim,		b2.num_APN3_3Trim,
		b2.num_apn,
		-- Sulfato Ferroso + Acido Folico
		b3.Fecha_Entrega1_SfAF,		b3.num_Entrega1_SfAf,
		b3.Fecha_Entrega2_SfAF,		b3.num_Entrega2_SfAf,
		b3.Fecha_Entrega3_SfAF,		b3.num_Entrega3_SfAf,
		b3.Fecha_Entrega4_SfAF,		b3.num_Entrega4_SfAf,
		b3.Fecha_Entrega5_SfAF,		b3.num_Entrega5_SfAf,
		b3.num_entrega_SfAf
from #denominador a
left join #num_exam_aux			b1	on	a.num_doc=b1.num_doc and a.ubigeo_reniec=b1.ubigeo_reniec and a.fecha_parto=b1.fecha_parto and a.semana_nac=b1.semana_nac
left join #num_apn				b2	on	a.num_doc=b2.num_doc and a.ubigeo_reniec=b2.ubigeo_reniec and a.fecha_parto=b2.fecha_parto and a.semana_nac=b2.semana_nac	
left join #num_entrega_SfAf		b3	on	a.num_doc=b3.num_doc and a.ubigeo_reniec=b3.ubigeo_reniec and a.fecha_parto=b3.fecha_parto and a.semana_nac=b3.semana_nac	

print(@mes_inicio)
print(@año)

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
if object_id(N'FED25.TRAMAHIS_FED2024_25_MC01_nominal', N'U') is not null drop table FED25.TRAMAHIS_FED2024_25_MC01_nominal;
select convert(char(4),año)+right('00'+convert(nvarchar(2),mes),2) Periodo
, b.UBIGEO ubigeo_inei
, b.GR_DIRIS diresa
,case
		When left(b.des_pro,4) = '1501' and b.des_dpo = '15 LIMA' then '15 LIMA METROPOLITANA'
		When b.des_dpo = '15 LIMA'  and left(b.des_pro,4) <>'1501' then '15 LIMA PROVINCIAS'
	Else b.des_dpo  End as departamento
, b.DES_PRO provincia
, b.DES_DIS distrito
, isnull(b.fed,0) Distritos_FED_23_24
, isnull(b.fed_limametro,0) Distritos_FED_23_24_LimaMetro 
, isnull(b.Fed_2018,0) Distritos_FED_24_25
, isnull(b.fed_IC,0) Distritos_FED_IC
, a.*
into FED25.TRAMAHIS_FED2024_25_MC01_nominal
from #reporte_final a
inner join MaeUbigeo_20240808 b on try_convert(int,a.ubigeo_reniec)=try_convert(int,b.ubigeo_reniec)

-- Tabla: Reporte Consolidado
select periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, flag_indicador, Gest_37sem, EESS_parto, sector
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
, sum(denominador) denominador
, sum(numerador) numerador
, sum(num_exam_hb)				num_exam_hb
, sum(num_exam_sifilis)			num_exam_sifilis
, sum(num_exam_VIH)				num_exam_VIH
, sum(num_exam_bacteriuria)		num_exam_bacteriuria
, sum(num_perfil_obstetrico)	num_perfil_obstetrico
, sum(num_exam_aux)				num_exam_aux
, sum(num_APN1_1trim)			num_APN1_1trim
, sum(num_APN1_2trim)			num_APN1_2trim
, sum(num_APN2_2trim)			num_APN2_2trim
, sum(num_APN1_3trim)			num_APN1_3trim
, sum(num_APN2_3trim)			num_APN2_3trim
, sum(num_APN3_3trim)			num_APN3_3trim
, sum(num_APN)					num_APN
, sum(num_entrega1_SfAf)		num_entrega1_SfAf
, sum(num_entrega2_SfAf)		num_entrega2_SfAf
, sum(num_entrega3_SfAf)		num_entrega3_SfAf
, sum(num_entrega4_SfAf)		num_entrega4_SfAf
, sum(num_entrega5_SfAf)		num_entrega5_SfAf
, sum(num_entrega_SfAf)			num_entrega_SfAf
from FED25.TRAMAHIS_FED2024_25_MC01_nominal
group by periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, flag_indicador, Gest_37sem, EESS_parto, sector
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
------------------------------------------- =D


