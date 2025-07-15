/*=======================================================================================================================================
Compromiso Gestion: SI-03 | Niñas y niños menores de 12 meses de edad del Peru, que cuentan con controles CRED según edad.

Indicador	:	SI-03.01  | Porcentaje de niñas y niños menores de 12 meses de edad del Perú, que cuentan con controles CRED según edad.

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

- 1.HIS MINSA (Mes de evalucación)			| Cambia mes a mes.
- 2.HIS MINSA (Año Previo)
- 3.Padron Nominal (Mes de Evaluación)		| Cambia mes a mes.
- 4.CNV (Mes de Evaluación)					| Cambia mes a mes.
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
					'99381',			-- Códigos de CRED.
					'Z001'				-- Códigos de CRED (Antiguo).
				)
and sw=1					-- Validación del numero de documento.
and id_tipo_doc in (1,6)	-- Tipo de documento: DNI o CNV.


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
					'99381',			-- Códigos de CRED.
					'Z001'				-- Códigos de CRED (Antiguo).
					)
and sw=1					-- Validación del numero de documento.
and id_tipo_doc in (1,6)	-- Tipo de documento: DNI o CNV.


-- 3.Tabla de datos : Padron Nominal (Mes de evaluación)
if Object_id(N'tempdb..#padron_nominal',N'U') is not null drop table #padron_nominal;
select *
into #padron_nominal
from (
	select distinct case when nu_dni_menor is null or nu_dni_menor in ('','null') then nu_cnv	
					else nu_dni_menor end num_doc,
					case when nu_dni_menor is null or nu_dni_menor in ('','null') then 'CNV'
					else 'DNI' end tipo_doc,
					co_ubigeo_inei ubigeo,
					seguro= case 
					when convert(int,TI_SEGURO_MENOR)='1' then 'MINSA'	
					when convert(int,TI_SEGURO_MENOR)='2' then 'ESSALUD'
					when convert(int,TI_SEGURO_MENOR)='3' then 'SANIDAD FFAA/PNP' 
					when convert(int,TI_SEGURO_MENOR)='4' then 'PRIVADO'
					else 'SIN REGISTRO' END,
					convert(date,fe_nac_menor) fecha_nac,
					case when DE_GENERO_MENOR='FEMENINO' then 'F' else 'M' end sexo
	from BD_BACKUP_OGEI.dbo.TramaPadronNominal with (nolock)
) as t 

		--------------------------------------------
		--- Eliminar registros vacios o nulos ---
		delete from #padron_nominal
		where num_doc is null or num_doc=''

		--------------------------------------------
		--- Eliminar registros Duplicados ---
		delete from #padron_nominal
		where num_doc in (
					select num_doc
					from #padron_nominal
					group by num_doc 
					having count(*)>1
					) 
				and tipo_doc not in ('DNI')
		----------------------------------------


-- 4.Tabla de datos : CNV 
if Object_id(N'tempdb..#cnv',N'U') is not null drop table #cnv;
select distinct nu_cnv num_doc ,
				try_convert(int,peso_nacido)	Peso,
				try_convert(int,dur_emb_parto)	Sem_Gestacion
into #cnv 
from BD_BACKUP_OGEI.dbo.TramaCNV with (nolock)
where sw_cnv=1										-- Validación del numero de documento.

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
año						int,
mes						int,
tipo_doc				nvarchar(3),
num_doc					nvarchar(15),
fecha_nac				date,
sexo					nvarchar(1),
ubigeo					int,
seguro					nvarchar(30),
fecha_final				date,
edad_dias				int,
edad_mes				int,
flag_364				int,				
flag_cnv				int,
peso_cnv				int,
flag_BPN				int,
Semana_gest_cnv			int,
flag_prematuro			int,
flag_BPN_Prematuro		int,
flag_indicador			int,
fecha_cred_mes1  		date,
num_cred_mes1  			int,
fecha_cred_mes2  		date,
num_cred_mes2  			int,
fecha_cred_mes3  		date,
num_cred_mes3  			int,
fecha_cred_mes4  		date,
num_cred_mes4  			int,
fecha_cred_mes5  		date,
num_cred_mes5  			int,
fecha_cred_mes6  		date,
num_cred_mes6  			int,
fecha_cred_mes7  		date,
num_cred_mes7  			int,
fecha_cred_mes8  		date,
num_cred_mes8  			int,
fecha_cred_mes9  		date,
num_cred_mes9  			int,
fecha_cred_mes10  		date,
num_cred_mes10  		int,
fecha_cred_mes11  		date,
num_cred_mes11  		int,
numerador				float,
denominador				float
)
		
declare @mes_inicio int, 
		@mes_eval int, 
		@año int 

set @año=2025
set @mes_inicio=1 
set @mes_eval= 1 --< Modificar segun mes de evaluación.

while @mes_inicio <= @mes_eval
begin

--%%%%%%%%%%%%%%%%
-- 1.DENOMINADOR
--%%%%%%%%%%%%%%%%

/* 1.1 - Niñas y niños menores de 12 meses de edad (364 días) del Padron Nominal, en el mes de medición
-// Se considera a los niños entre 29 días a 11 meses 29 días (364 dias) de edad, para el proceso de verificación //
-// La determinación del corte de edad para cada periodo de medición, será el último día de cada mes. //					*/
if Object_id(N'tempdb..#padron_cred',N'U') is not null drop table #padron_cred;
select distinct tipo_doc, num_doc, fecha_nac, sexo, ubigeo, seguro, fecha_final, edad_dias, edad_mes
, case when edad_dias=364 then 1 else 0 end flag_364
into #padron_cred
from (
		select *
		, datediff(dd,fecha_nac,fecha_final) edad_dias		-- La determinación del corte de edad para cada periodo de medición, será el último día de cada mes.
		, dbo.fn_calcula_edadmeses(fecha_nac,fecha_final) as edad_mes
		from (
			select	tipo_doc,
					num_doc,
					fecha_nac,
					sexo,
					ubigeo,
					seguro, 
					eomonth(try_convert(date,try_convert(varchar(4),@año)+'-'+right('00'+try_convert(varchar(2),@mes_inicio),2)+'-'+right('00'+try_convert(varchar(2),1),2))) fecha_final			-- Ultimo dia de cada mes.
			from #padron_nominal
			where fecha_nac<=eomonth(try_convert(date,try_convert(varchar(4),@año)+'-'+right('00'+try_convert(varchar(2),@mes_inicio),2)+'-'+right('00'+try_convert(varchar(2),1),2)))
		) as t0
	union all
		select *
		, datediff(dd,fecha_nac,fecha_final) edad_dias		-- La determinación del corte de edad para cada periodo de medición, será el último día de cada mes.
		, dbo.fn_calcula_edadmeses(fecha_nac,fecha_final) as edad_mes
		from (
			select	tipo_doc,
					num_doc,
					fecha_nac,
					sexo,
					ubigeo,
					seguro, 
					dateadd(dd,364,fecha_nac) fecha_final			-- Niños que cumplen 364 en el mes de evaluación.
			from #padron_nominal
			where year(dateadd(dd,364,fecha_nac))=@año
			and month(dateadd(dd,364,fecha_nac))=@mes_inicio
		) as t0
	) as t1
where (edad_dias between 29 and 364)	-- Se considera a los niños entre 29 días a 11 meses 29 días (364 dias) de edad.


/* 1.2 - Se excluye a niños y niñas con bajo peso al nacer (menor de 2500 gramos) y/o prematuros (menor de 37 SG)
, registrados en CNV en línea		*/
if Object_id(N'tempdb..#cnv_bpn_premat',N'U') is not null drop table #cnv_bpn_premat;
select num_doc
, max(peso)	peso
, max(sem_gestacion) sem_gestacion
, max(iif(peso<2500,1,0)) BPN											-- Bajo Peso al Nacer
, max(iif(Sem_Gestacion<37,1,0)) Prematuro								-- Prematuro al Nacer
, max(iif( (peso<2500) or (Sem_Gestacion<37),1,0))	BPN_Premat			-- Bajo Peso o Prematuro al Nacer
into #cnv_bpn_premat
from #cnv
group by num_doc

	
/* 1.3 - Unión de Información para armar el denominador del indicador. */
if Object_id(N'tempdb..#denominador',N'U') is not null drop table #denominador;
select a.*
, iif(b.num_doc is null,0,1)				flag_cnv
, isnull(b.Peso,0)							Peso_cnv
, isnull(b.BPN,0)							flag_BPN
, isnull(b.Sem_Gestacion,0)					Semana_gest_cnv
, isnull(b.Prematuro,0)						flag_prematuro
, isnull(b.BPN_Premat,0)					flag_BPN_Prematuro
, iif(b.BPN_Premat is null and flag_364=1,1,0)flag_indicador
into #denominador
from #padron_cred a
left join #cnv_bpn_premat b on a.num_doc=b.num_doc


--%%%%%%%%%%%%%%%%
-- 2.NUMERADOR
--%%%%%%%%%%%%%%%%

/*==============================================================================
		--------------------------------------
		Tabla de clasificación y cumplimiento
		-------------------------------------
N°		Grupo de edad	Rango de edad	Cumple					
1		1 MES			60-89			1
2		2 MESES			90-119			1+1
3		3 MESES			120-149			1+1+1
4		4 MESES			150-179			1+1+1+1
5		5 MESES			180-209			1+1+1+1+1
6		6 MESES			210-239			1+1+1+1+1+1
7		7 MESES			240-269			1+1+1+1+1+1+1
8		8 MESES			270-299			1+1+1+1+1+1+1+1
9		9 MESES			300-329			1+1+1+1+1+1+1+1+1
10		10 MESES		330-363			1+1+1+1+1+1+1+1+1+1
11		11 MESES		364				1+1+1+1+1+1+1+1+1+1+1   

==============================================================================  */

/* 2.1 - Niños del denominador que cuentan con el primer Control CRED.
	# El primer control es a partir de los 29 días.
	# Se contabiliza los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_mes1',N'U') is not null drop table #cred_mes1;
select	a.tipo_doc, 
		a.num_doc, 
		a.fecha_nac, 
		a.ubigeo, 
		a.seguro, 
		a.fecha_final,
		min(b.fecha_atencion) fecha_cred_mes1,
		num_cred_mes1=1
into #cred_mes1
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('Z001','99381')								-- Códigos de CRED.
and b.fecha_atencion<=a.fecha_final
and (datediff(dd,fecha_nac,fecha_atencion) between 29 and 59)		-- El primer control es a partir de los 29 días hasta un dia antes del inicio del proximo rango de edad edad.
group by a.tipo_doc, a.num_doc, a.fecha_nac, 
		a.ubigeo, a.seguro, a.fecha_final


/* 2.2 - Niños del denominador que cuentan con el Segundo Control CRED.
	# Se contabiliza los controles CRED, independientemente del LAB registrado.
	# Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control. */
if Object_id(N'tempdb..#cred_mes2',N'U') is not null drop table #cred_mes2;
select	a.tipo_doc, 
		a.num_doc, 
		a.fecha_nac, 
		a.ubigeo, 
		a.seguro, 
		a.fecha_final,
		min(b.fecha_atencion) fecha_cred_mes2,
		num_cred_mes2=1
into #cred_mes2
from #cred_mes1 a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('Z001','99381')								-- Códigos de CRED.
and b.fecha_atencion<=a.fecha_final
and (datediff(dd,fecha_nac,fecha_atencion) between 60 and 89)		-- El Segundo control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,fecha_cred_mes1,fecha_atencion)>=28					-- Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control.
group by a.tipo_doc, a.num_doc, a.fecha_nac, 
		a.ubigeo, a.seguro, a.fecha_final


/* 2.3 - Niños del denominador que cuentan con el Tercer Control CRED.
	# Se contabiliza los controles CRED, independientemente del LAB registrado.
	# Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control. */
if Object_id(N'tempdb..#cred_mes3',N'U') is not null drop table #cred_mes3;
select	a.tipo_doc, 
		a.num_doc, 
		a.fecha_nac, 
		a.ubigeo, 
		a.seguro, 
		a.fecha_final,
		min(b.fecha_atencion) fecha_cred_mes3,
		num_cred_mes3=1
into #cred_mes3
from #cred_mes2 a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('Z001','99381')								-- Códigos de CRED.
and b.fecha_atencion<=a.fecha_final
and (datediff(dd,fecha_nac,fecha_atencion) between 90 and 119)		-- El Tercer control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,fecha_cred_mes2,fecha_atencion)>=28					-- Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control.
group by a.tipo_doc, a.num_doc, a.fecha_nac, 
		a.ubigeo, a.seguro, a.fecha_final


/* 2.4 - Niños del denominador que cuentan con el Cuarto Control CRED.
	# Se contabiliza los controles CRED, independientemente del LAB registrado.
	# Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control. */
if Object_id(N'tempdb..#cred_mes4',N'U') is not null drop table #cred_mes4;
select	a.tipo_doc, 
		a.num_doc, 
		a.fecha_nac, 
		a.ubigeo, 
		a.seguro, 
		a.fecha_final,
		min(b.fecha_atencion) fecha_cred_mes4,
		num_cred_mes4=1
into #cred_mes4
from #cred_mes3 a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('Z001','99381')								-- Códigos de CRED.
and b.fecha_atencion<=a.fecha_final
and (datediff(dd,fecha_nac,fecha_atencion) between 120 and 149)		-- El Cuarto control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,fecha_cred_mes3,fecha_atencion)>=28					-- Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control.
group by a.tipo_doc, a.num_doc, a.fecha_nac, 
		a.ubigeo, a.seguro, a.fecha_final


/* 2.5 - Niños del denominador que cuentan con el Quinto Control CRED.
	# Se contabiliza los controles CRED, independientemente del LAB registrado.
	# Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control. */
if Object_id(N'tempdb..#cred_mes5',N'U') is not null drop table #cred_mes5;
select	a.tipo_doc, 
		a.num_doc, 
		a.fecha_nac, 
		a.ubigeo, 
		a.seguro, 
		a.fecha_final,
		min(b.fecha_atencion) fecha_cred_mes5,
		num_cred_mes5=1
into #cred_mes5
from #cred_mes4 a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('Z001','99381')								-- Códigos de CRED.
and b.fecha_atencion<=a.fecha_final
and (datediff(dd,fecha_nac,fecha_atencion) between 150 and 179)		-- El Quinto control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,fecha_cred_mes4,fecha_atencion)>=28					-- Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control.
group by a.tipo_doc, a.num_doc, a.fecha_nac, 
		a.ubigeo, a.seguro, a.fecha_final


/* 2.6 - Niños del denominador que cuentan con el Sexto Control CRED.
	# Se contabiliza los controles CRED, independientemente del LAB registrado.
	# Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control. */
if Object_id(N'tempdb..#cred_mes6',N'U') is not null drop table #cred_mes6;
select	a.tipo_doc, 
		a.num_doc, 
		a.fecha_nac, 
		a.ubigeo, 
		a.seguro, 
		a.fecha_final,
		min(b.fecha_atencion) fecha_cred_mes6,
		num_cred_mes6=1
into #cred_mes6
from #cred_mes5 a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('Z001','99381')								-- Códigos de CRED.
and b.fecha_atencion<=a.fecha_final
and (datediff(dd,fecha_nac,fecha_atencion) between 180 and 209)		-- El Sexto control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,fecha_cred_mes5,fecha_atencion)>=28					-- Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control.
group by a.tipo_doc, a.num_doc, a.fecha_nac, 
		a.ubigeo, a.seguro, a.fecha_final


/* 2.7 - Niños del denominador que cuentan con el Septimo Control CRED.
	# Se contabiliza los controles CRED, independientemente del LAB registrado.
	# Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control. */
if Object_id(N'tempdb..#cred_mes7',N'U') is not null drop table #cred_mes7;
select	a.tipo_doc, 
		a.num_doc, 
		a.fecha_nac, 
		a.ubigeo, 
		a.seguro, 
		a.fecha_final,
		min(b.fecha_atencion) fecha_cred_mes7,
		num_cred_mes7=1
into #cred_mes7
from #cred_mes6 a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('Z001','99381')								-- Códigos de CRED.
and b.fecha_atencion<=a.fecha_final
and (datediff(dd,fecha_nac,fecha_atencion) between 210 and 239)		-- El Septimo control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,fecha_cred_mes6,fecha_atencion)>=28					-- Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control.
group by a.tipo_doc, a.num_doc, a.fecha_nac, 
		a.ubigeo, a.seguro, a.fecha_final


/* 2.8 - Niños del denominador que cuentan con el Octavo Control CRED.
	# Se contabiliza los controles CRED, independientemente del LAB registrado.
	# Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control. */
if Object_id(N'tempdb..#cred_mes8',N'U') is not null drop table #cred_mes8;
select	a.tipo_doc, 
		a.num_doc, 
		a.fecha_nac, 
		a.ubigeo, 
		a.seguro, 
		a.fecha_final,
		min(b.fecha_atencion) fecha_cred_mes8,
		num_cred_mes8=1
into #cred_mes8
from #cred_mes7 a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('Z001','99381')								-- Códigos de CRED.
and b.fecha_atencion<=a.fecha_final
and (datediff(dd,fecha_nac,fecha_atencion) between 240 and 269)		-- El Octavo control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,fecha_cred_mes7,fecha_atencion)>=28					-- Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control.
group by a.tipo_doc, a.num_doc, a.fecha_nac, 
		a.ubigeo, a.seguro, a.fecha_final


/* 2.9 - Niños del denominador que cuentan con el Noveno Control CRED.
	# Se contabiliza los controles CRED, independientemente del LAB registrado.
	# Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control. */
if Object_id(N'tempdb..#cred_mes9',N'U') is not null drop table #cred_mes9;
select	a.tipo_doc, 
		a.num_doc, 
		a.fecha_nac, 
		a.ubigeo, 
		a.seguro, 
		a.fecha_final,
		min(b.fecha_atencion) fecha_cred_mes9,
		num_cred_mes9=1
into #cred_mes9
from #cred_mes8 a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('Z001','99381')								-- Códigos de CRED.
and b.fecha_atencion<=a.fecha_final
and (datediff(dd,fecha_nac,fecha_atencion) between 270 and 299)		-- El Noveno control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,fecha_cred_mes8,fecha_atencion)>=28					-- Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control.
group by a.tipo_doc, a.num_doc, a.fecha_nac, 
		a.ubigeo, a.seguro, a.fecha_final


/* 2.10 - Niños del denominador que cuentan con el Decimo Control CRED.
	# Se contabiliza los controles CRED, independientemente del LAB registrado.
	# Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control. */
if Object_id(N'tempdb..#cred_mes10',N'U') is not null drop table #cred_mes10;
select	a.tipo_doc, 
		a.num_doc, 
		a.fecha_nac, 
		a.ubigeo, 
		a.seguro, 
		a.fecha_final,
		min(b.fecha_atencion) fecha_cred_mes10,
		num_cred_mes10=1
into #cred_mes10
from #cred_mes9 a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('Z001','99381')								-- Códigos de CRED.
and b.fecha_atencion<=a.fecha_final
and (datediff(dd,fecha_nac,fecha_atencion) between 300 and 329)		-- El Decimo control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,fecha_cred_mes9,fecha_atencion)>=28					-- Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control.
group by a.tipo_doc, a.num_doc, a.fecha_nac, 
		a.ubigeo, a.seguro, a.fecha_final


/* 2.11 - Niños del denominador que cuentan con el Onceavo Control CRED.
	# Se contabiliza los controles CRED, independientemente del LAB registrado.
	# Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control. */
if Object_id(N'tempdb..#cred_mes11',N'U') is not null drop table #cred_mes11;
select	a.tipo_doc, 
		a.num_doc, 
		a.fecha_nac, 
		a.ubigeo, 
		a.seguro, 
		a.fecha_final,
		min(b.fecha_atencion) fecha_cred_mes11,
		num_cred_mes11=1
into #cred_mes11
from #cred_mes10 a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('Z001','99381')								-- Códigos de CRED.
and b.fecha_atencion<=a.fecha_final
and (datediff(dd,fecha_nac,fecha_atencion) between 330 and 364)		-- El Onceavo control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,fecha_cred_mes10,fecha_atencion)>=28				-- Para efectos de búsqueda de datos se considera un mínimo de 28 dias, entre control y control.
group by a.tipo_doc, a.num_doc, a.fecha_nac, 
		a.ubigeo, a.seguro, a.fecha_final


--%%%%%%%%%%%%%%%%
--	3.INDICADOR
--%%%%%%%%%%%%%%%% 

--3.1 - Se procede a unir el denominador con los atributos del numerador para elaborar el reporte final.
if Object_id(N'tempdb..#cred_indicador',N'U') is not null drop table #cred_indicador;
select a.*
, b1.fecha_cred_mes1			,isnull(b1.num_cred_mes1,0) num_cred_mes1
, b2.fecha_cred_mes2			,isnull(b2.num_cred_mes2,0) num_cred_mes2
, b3.fecha_cred_mes3			,isnull(b3.num_cred_mes3,0) num_cred_mes3
, b4.fecha_cred_mes4			,isnull(b4.num_cred_mes4,0) num_cred_mes4
, b5.fecha_cred_mes5			,isnull(b5.num_cred_mes5,0) num_cred_mes5
, b6.fecha_cred_mes6			,isnull(b6.num_cred_mes6,0) num_cred_mes6
, b7.fecha_cred_mes7			,isnull(b7.num_cred_mes7,0) num_cred_mes7
, b8.fecha_cred_mes8			,isnull(b8.num_cred_mes8,0) num_cred_mes8
, b9.fecha_cred_mes9			,isnull(b9.num_cred_mes9,0) num_cred_mes9
, b10.fecha_cred_mes10			,isnull(b10.num_cred_mes10,0) num_cred_mes10
, b11.fecha_cred_mes11			,isnull(b11.num_cred_mes11,0) num_cred_mes11
into #cred_indicador
from #denominador (nolock) a
left join #cred_mes1	b1		on a.seguro=b1.seguro and a.num_doc=b1.num_doc and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Tabla Cred Mensual 1
left join #cred_mes2	b2		on a.seguro=b2.seguro and a.num_doc=b2.num_doc and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo				-- Tabla Cred Mensual 2
left join #cred_mes3	b3		on a.seguro=b3.seguro and a.num_doc=b3.num_doc and a.fecha_nac=b3.fecha_nac and a.ubigeo=b3.ubigeo				-- Tabla Cred Mensual 3
left join #cred_mes4	b4		on a.seguro=b4.seguro and a.num_doc=b4.num_doc and a.fecha_nac=b4.fecha_nac and a.ubigeo=b4.ubigeo				-- Tabla Cred Mensual 4
left join #cred_mes5	b5		on a.seguro=b5.seguro and a.num_doc=b5.num_doc and a.fecha_nac=b5.fecha_nac and a.ubigeo=b5.ubigeo				-- Tabla Cred Mensual 5
left join #cred_mes6	b6		on a.seguro=b6.seguro and a.num_doc=b6.num_doc and a.fecha_nac=b6.fecha_nac and a.ubigeo=b6.ubigeo				-- Tabla Cred Mensual 6
left join #cred_mes7	b7		on a.seguro=b7.seguro and a.num_doc=b7.num_doc and a.fecha_nac=b7.fecha_nac and a.ubigeo=b7.ubigeo				-- Tabla Cred Mensual 7
left join #cred_mes8	b8		on a.seguro=b8.seguro and a.num_doc=b8.num_doc and a.fecha_nac=b8.fecha_nac and a.ubigeo=b8.ubigeo				-- Tabla Cred Mensual 8
left join #cred_mes9	b9		on a.seguro=b9.seguro and a.num_doc=b9.num_doc and a.fecha_nac=b9.fecha_nac and a.ubigeo=b9.ubigeo				-- Tabla Cred Mensual 9
left join #cred_mes10	b10		on a.seguro=b10.seguro and a.num_doc=b10.num_doc and a.fecha_nac=b10.fecha_nac and a.ubigeo=b10.ubigeo			-- Tabla Cred Mensual 10
left join #cred_mes11	b11		on a.seguro=b11.seguro and a.num_doc=b11.num_doc and a.fecha_nac=b11.fecha_nac and a.ubigeo=b11.ubigeo			-- Tabla Cred Mensual 11


-- 3.2 - Insertar Información a Tabla Reporte.
insert into #tabla_reporte
select @año año, @mes_inicio mes 
, *
,case when edad_dias<=59 then 1
		when (edad_dias between 60 and 89) and num_cred_mes1>=1 then 1
		when (edad_dias between 90 and 119) and num_cred_mes1+num_cred_mes2>=2 then 1
		when (edad_dias between 120 and 149) and num_cred_mes1+num_cred_mes2+num_cred_mes3>=3 then 1
		when (edad_dias between 150 and 179) and num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4>=4 then 1
		when (edad_dias between 180 and 209) and num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4+num_cred_mes5>=5 then 1
		when (edad_dias between 210 and 239) and num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4+num_cred_mes5+num_cred_mes6>=6 then 1
		when (edad_dias between 240 and 269) and num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4+num_cred_mes5+num_cred_mes6+num_cred_mes7>=7 then 1
		when (edad_dias between 270 and 299) and num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4+num_cred_mes5+num_cred_mes6+num_cred_mes7+num_cred_mes8>=8 then 1
		when (edad_dias between 300 and 329) and num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4+num_cred_mes5+num_cred_mes6+num_cred_mes7+num_cred_mes8+num_cred_mes9>=9 then 1
		when (edad_dias between 330 and 363) and num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4+num_cred_mes5+num_cred_mes6+num_cred_mes7+num_cred_mes8+num_cred_mes9+num_cred_mes10>=10 then 1
		when edad_dias>=364 and num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4+num_cred_mes5+num_cred_mes6+num_cred_mes7+num_cred_mes8+num_cred_mes9+num_cred_mes10+num_cred_mes11>=11 then 1 else 0 end numerador  -- Cumplimiento de Cred Segun Edad.
, denominador=1
from #cred_indicador


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
if object_id(N'FED25.TRAMAHIS_FED2024_25_SI0301_Nominal_2025', N'U') is not null drop table FED25.TRAMAHIS_FED2024_25_SI0301_Nominal_2025;
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
into FED25.TRAMAHIS_FED2024_25_SI0301_Nominal_2025
-- into #TRAMAHIS_FED2024_25_SI0301_Nominal
from #tabla_reporte a
inner join MaeUbigeo_20240808 b on convert(int,a.ubigeo)=convert(int,b.ubigeo)

-- Tabla: Reporte Consolidado
select periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, flag_indicador, flag_BPN, flag_prematuro, flag_BPN_Prematuro, edad_mes, seguro, sexo, flag_364
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
, sum(denominador) denominador
, sum(numerador) numerador
, sum(num_cred_mes1) cred_mes_1
, sum(num_cred_mes2) cred_mes_2
, sum(num_cred_mes3) cred_mes_3
, sum(num_cred_mes4) cred_mes_4
, sum(num_cred_mes5) cred_mes_5
, sum(num_cred_mes6) cred_mes_6
, sum(num_cred_mes7) cred_mes_7
, sum(num_cred_mes8) cred_mes_8
, sum(num_cred_mes9) cred_mes_9
, sum(num_cred_mes10) cred_mes_10
, sum(num_cred_mes11) cred_mes_11
-- from #TRAMAHIS_FED2024_25_SI0301_Nominal
from FED25.TRAMAHIS_FED2024_25_SI0301_Nominal_2025
group by periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, flag_indicador, flag_BPN, flag_prematuro, flag_BPN_Prematuro, edad_mes, seguro, sexo, flag_364
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
------------------------------------- =D
