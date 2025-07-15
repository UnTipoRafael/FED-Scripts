/*=======================================================================================================================================
Compromiso Gestion: MC-02 | Niñas y Niños menores de 12 meses de edad procedentes de los quintiles 1 y 2 de 
							pobreza departamental que reciben el paquete integrado de servicios.

Indicador	:	MC-02.01  | Porcentaje de niñas y niños menores de 12 meses de edad procedentes de distritos de quintiles 1 y 2 de pobreza departamental
							, que recibieron el paquete integado de servicios.

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
use BD_HISINDICADORES
go

/* **********************************************************
				BASES DE DATOS.

En esta parte se procede a seleccionar las bases de datos 
utilizadas para generar el indicador. Las bases de datos utilizadas 
para este indicador son:

- HIS MINSA			(Mes de evalucación)
- HIS MINSA			(Año anterior)
- Padron Nominal	(Mes de evaluación)
- CNV				(Mes de evaluación) 
- EMISION DNI		(Mes de evaluación) 
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
					'Z001','99381','99381.01'					-- Códigos de CRED
					,'90670'									-- Código de vacuna Antineumococica
					,'90681'									-- Código de vacuna Rotavirus			
					,'90712','90713'							-- Código de vacuna Antipolio
					,'90723','90722'							-- Código de Pentavalente
					,'99199.17','99199.19','99199.11'			-- Código de Suplementación/Tratamiento de Hierro/multimicronutrientes
					,'85018','85018.01'							-- Código de dosaje 
					,'D500','D508','D509','D649','D539'			-- Código de Dx de Anemia
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
					'Z001','99381','99381.01'					-- Códigos de CRED
					,'90670'									-- Código de vacuna Antineumococica
					,'90681'									-- Código de vacuna Rotavirus			
					,'90712','90713'							-- Código de vacuna Antipolio
					,'90723','90722'							-- Código de Pentavalente
					,'99199.17','99199.19','99199.11'			-- Código de Suplementación/Tratamiento de Hierro/multimicronutrientes
					,'85018','85018.01'							-- Código de dosaje 
					,'D500','D508','D509','D649','D539'			-- Código de Dx de Anemia
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


-- 5.Tabla de datos : EMISION DNI
if Object_id(N'tempdb..#emision',N'U') is not null drop table #emision;
select tdoc=1
	, dni num_doc
	, convert(date,fec_emision) fecha_emision 
into #emision
from .dbo.DNI_FEC_EMISION_202411
where try_convert(int,dni) is not null -- NO SE CONSIDERAN DOCUMENTOS NULOS 


--***************************************************
--					SINTAXIS
--***************************************************
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
edad_dias				int,
edad_mes				int,
flag_cnv				int,
peso_cnv				int,
flag_BPN				int,
Semana_gest_cnv			int,
flag_prematuro			int,
flag_BPN_Prematuro		int,
flag_indicador			int,
denominador				int,
numerador				int,
num_cred				int,
num_cred_rn				int,
fecha_cred_rn1			date,
num_cred_rn1			int,
fecha_cred_rn2			date,
num_cred_rn2			int,
fecha_cred_rn3			date,
num_cred_rn3			int,
fecha_cred_rn4			date,
num_cred_rn4			int,
num_cred_mensual		int,
fecha_cred_mes1			date,
num_cred_mes1			int,
fecha_cred_mes2			date,
num_cred_mes2			int,
fecha_cred_mes3			date,
num_cred_mes3			int,
fecha_cred_mes4			date,
num_cred_mes4			int,
fecha_cred_mes5			date,
num_cred_mes5			int,
fecha_cred_mes6			date,
num_cred_mes6			int,
fecha_cred_mes7			date,
num_cred_mes7			int,
fecha_cred_mes8			date,
num_cred_mes8			int,
fecha_cred_mes9			date,
num_cred_mes9			int,
fecha_cred_mes10		date,
num_cred_mes10			int,
fecha_cred_mes11		date,
num_cred_mes11			int,
num_vac						int,
num_vac_antineumococica		int,
fecha_vac_antineumococica1	date,
num_vac_antineumococica1	int,
fecha_vac_antineumococica2	date,
num_vac_antineumococica2	int,
num_vac_antipolio			int,
fecha_vac_antipolio1		date,
num_vac_antipolio1			int,
fecha_vac_antipolio2		date,
num_vac_antipolio2			int,
fecha_vac_antipolio3		date,
num_vac_antipolio3			int,
num_vac_pentavalente		int,
fecha_vac_pentavalente1		date,
num_vac_pentavalente1		int,
fecha_vac_pentavalente2		date,
num_vac_pentavalente2		int,
fecha_vac_pentavalente3		date,
num_vac_pentavalente3		int,
num_vac_rotavirus			int,
fecha_vac_rotavirus1		date,
num_vac_rotavirus1			int,
fecha_vac_rotavirus2		date,
num_vac_rotavirus2			int,
num_esq						int,
num_esq4M					int,
fecha_Esq4m_sup_E1			date,
num_Esq4m_sup_E1			int,
num_esq6M					int,
num_esq6M_sup				int,
fecha_Esq6m_sup_E1			date,
num_Esq6m_sup_E1			int,
fecha_Esq6m_sup_E2			date,
num_Esq6m_sup_E2			int,
num_esq6M_trat				int,
fecha_Esq6m_trat_E1			date,
num_Esq6m_trat_E1			int,
fecha_Esq6m_trat_E2			date,
num_Esq6m_trat_E2			int,
fecha_Esq6m_trat_E3			date,
num_Esq6m_trat_E3			int,
num_esq6M_multi				int,
fecha_Esq6m_multi_E1		date,
num_Esq6m_multi_E1			int,
fecha_Esq6m_multi_E2		date,
num_Esq6m_multi_E2			int,
fecha_Esq6m_multi_E3		date,
num_Esq6m_multi_E3			int,
fecha_Esq6m_multi_E4		date,
num_Esq6m_multi_E4			int,
fecha_Esq6m_multi_E5		date,
num_Esq6m_multi_E5			int,
fecha_Esq6m_multi_E6		date,
num_Esq6m_multi_E6			int,
num_dosaje_Hb				int,
fecha_Hb					date,
num_Hb						int
,num_DNIemision				int,
fecha_DNIemision			date,
num_DNIemision_30d			int,
num_DNIemision_60d			int
)
	
declare @mes_inicio int, 
		@mes_eval int, 
		@año int 

set @año=2025
set @mes_inicio= 1 
set @mes_eval= 1 --< Modificar segun mes de evaluación.

while @mes_inicio <= @mes_eval
begin

    -- Realiza alguna operación
    -- RAISERROR('Iteración número: %d', 0, 1, @counter) WITH NOWAIT;

--%%%%%%%%%%%%%%%%
-- 1.DENOMINADOR
--%%%%%%%%%%%%%%%%

/* 1.1 - Niñas y niños menores de 12 meses de edad (364 días) del Padron Nominal, en el mes de medición
-// La determinación del corte de edad para cada periodo de medición, será el último día de cada mes. //					*/
if Object_id(N'tempdb..#padron_12m',N'U') is not null drop table #padron_12m;
select *
into #padron_12m
from (
		select *
		, datediff(dd,fecha_nac,fecha_final) edad_dias																			-- La determinación del corte de edad para cada periodo de medición, será el último día de cada mes.
		, dbo.fn_Calcula_EdadMeses(fecha_nac,fecha_final) as edad_mes
		from (
			select	tipo_doc,
					num_doc,
					fecha_nac,
					sexo,
					ubigeo,
					seguro, 
					eomonth(try_convert(date,try_convert(varchar(4),@año)+'-'+right('00'+try_convert(varchar(2),@mes_inicio),2)+'-'+right('00'+try_convert(varchar(2),1),2))) fecha_final			-- Ultimo dia de cada mes.
			from #padron_nominal
		) as t0
) as t1
where (edad_dias between 0 and 364)	-- Se considera a los niños menores de 12 meses de edad (364 dias).



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
, iif(b.num_doc is null,0,1)	flag_cnv
, isnull(b.Peso,0)				Peso_cnv
, isnull(b.BPN,0)				flag_BPN
, isnull(b.Sem_Gestacion,0)		Semana_gest_cnv
, isnull(b.Prematuro,0)			flag_prematuro
, isnull(b.BPN_Premat,0)		flag_BPN_Prematuro
, iif(b.BPN_Premat=1,0,1)		flag_indicador
into #denominador
from #padron_12m a
left join #cnv_bpn_premat b on a.num_doc=b.num_doc


--%%%%%%%%%%%%%%%%
-- 2.NUMERADOR
--%%%%%%%%%%%%%%%%

/* ===================================================================================================================================
					2.1 CRED RN 

		--------------------------------------
		Tabla de clasificación y cumplimiento
		-------------------------------------
N°		Grupo de edad	Rango de edad	Cumple					
1		RN1					29-59		1
2		RN2					29-59		1+1
3		RN3					29-59		1+1+1
4		RN4					29-59		1+1+1+1
====================================================================================================================================== */

/* 2.1.1 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. * El código para Recién Nacido es:  CIE Z001 o CPMS 99381.01.
// Para el caso de los controles CRED del RN, el primer control CRED, se realiza a partir del 3er día de vida. 
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_rn1',N'U') is not null drop table #cred_rn1;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_cred_rn1 
				, num_cred_rn1=1
into #cred_rn1 
from #denominador a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381.01')												-- Código de control CRED RN.
and datediff(dd,fecha_nac,fecha_atencion)>=3										-- El primer control CRED se realiza a partir del 3er día de vida.
and b.fecha_atencion<=dateadd(dd,28,a.fecha_nac)									-- El control de RN es hasta los 28 dias de edad. 
and b.fecha_atencion<=a.fecha_final													-- Atenciones buscadas hasta el ultimo dia del mes de evaluación. 
	

/* 2.1.2 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. * El código para Recién Nacido es:  CIE Z001 o CPMS 99381.01.
// El segundo control con intervalo mínimo 3 dias a partir del 1er control.
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_rn2',N'U') is not null drop table #cred_rn2;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_cred_rn2 
				, num_cred_rn2=1
into #cred_rn2 
from #cred_rn1 a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381.01')												-- Código de control CRED RN.
and datediff(dd,a.fecha_cred_rn1,b.fecha_atencion)>=3								-- El segundo control CRED con intervalo mínimo de 3 dias a partir del 1er control.
and b.fecha_atencion<=dateadd(dd,28,a.fecha_nac)									-- El control de RN es hasta los 28 dias de edad. 
and b.fecha_atencion<=a.fecha_final													-- Atenciones buscadas hasta el ultimo dia del mes de evaluación. 
	

/* 2.1.3 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. * El código para Recién Nacido es:  CIE Z001 o CPMS 99381.01.
// El tercer control CRED hasta el cuarto control CRED, se realiza un control cada semana (07 días), mínimo.  
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_rn3',N'U') is not null drop table #cred_rn3;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_cred_rn3 
				, num_cred_rn3=1
into #cred_rn3 
from #cred_rn2 a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381.01')												-- Código de control CRED RN.
and datediff(dd,a.fecha_cred_rn2,b.fecha_atencion)>=7								-- El tercer control CRED hasta el cuarto control CRED, se realiza un control cada semana (07 días), mínimo.
and b.fecha_atencion<=dateadd(dd,28,a.fecha_nac)									-- El control de RN es hasta los 28 dias de edad. 
and b.fecha_atencion<=a.fecha_final													-- Atenciones buscadas hasta el ultimo dia del mes de evaluación. 

	
/* 2.1.4 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. * El código para Recién Nacido es:  CIE Z001 o CPMS 99381.01.
// El tercer control CRED hasta el cuarto control CRED, se realiza un control cada semana (07 días), mínimo.  
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_rn4',N'U') is not null drop table #cred_rn4;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_cred_rn4 
				, num_cred_rn4=1
into #cred_rn4 
from #cred_rn3 a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381.01')												-- Código de control CRED RN.
and datediff(dd,a.fecha_cred_rn3,b.fecha_atencion)>=7								-- El tercer control CRED hasta el cuarto control CRED, se realiza un control cada semana (07 días), mínimo.
and b.fecha_atencion<=dateadd(dd,28,a.fecha_nac)									-- El control de RN es hasta los 28 dias de edad. 
and b.fecha_atencion<=a.fecha_final		


/* 2.1.5 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de Recien Nacido de acuerdo a su edad. */
if Object_id(N'tempdb..#num_CRED_RN',N'U') is not null drop table #num_CRED_RN;
select  *
, case	when edad_dias<=28 then 1
		when edad_dias>=29 and (num_cred_rn1+num_cred_rn2+num_cred_rn3+num_cred_rn4=4) then 1 else 0 end num_cred_rn				-- CRED de RN de acuerdo a su edad.
into #num_CRED_RN
from (
		select a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
		, min(b1.fecha_cred_rn1) fecha_cred_rn1		, max(isnull(b1.num_cred_rn1,0))	num_cred_rn1
		, min(b2.fecha_cred_rn2) fecha_cred_rn2		, max(isnull(b2.num_cred_rn2,0))	num_cred_rn2
		, min(b3.fecha_cred_rn3) fecha_cred_rn3		, max(isnull(b3.num_cred_rn3,0))	num_cred_rn3
		, min(b4.fecha_cred_rn4) fecha_cred_rn4		, max(isnull(b4.num_cred_rn4,0))	num_cred_rn4
		from #denominador	a
		left join #cred_rn1	b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo		-- Trama de 1° Cred de RN
		left join #cred_rn2	b2	on a.num_doc=b2.num_doc and a.seguro=b2.seguro and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo		-- Trama de 2° Cred de RN
		left join #cred_rn3	b3	on a.num_doc=b3.num_doc and a.seguro=b3.seguro and a.fecha_nac=b3.fecha_nac and a.ubigeo=b3.ubigeo		-- Trama de 3° Cred de RN
		left join #cred_rn4	b4	on a.num_doc=b4.num_doc and a.seguro=b4.seguro and a.fecha_nac=b4.fecha_nac and a.ubigeo=b4.ubigeo		-- Trama de 4° Cred de RN
		group by a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
) as t 


/* ===================================================================================================================================
					2.2 CRED MENSUAL

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
====================================================================================================================================== */

/* 2.2.1 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. 
// Para el caso de los controles CRED de 1 mes (29 días) a 11 meses 29 días (364 días) de edad, se considera un intervalo mensual entre control y control (para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control)
// Iniciando el 1er control CRED mensual a partir de los 29 día
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_mes1',N'U') is not null drop table #cred_mes1;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, min(b.fecha_atencion) fecha_cred_mes1 
				, num_cred_mes1=1
into #cred_mes1 
from #denominador a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381')												-- Código de control CRED Mensual.
and ( datediff(dd,fecha_nac,fecha_atencion) between 29 and 59 )						-- Iniciando el 1er control CRED mensual a partir de los 29 días hasta un dia antes del inicio del proximo rango de edad edad.
and b.fecha_atencion<=a.fecha_final			
group by a.num_doc , a.fecha_nac , a.seguro
		, a.ubigeo, a.fecha_final


/* 2.2.2 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. 
// Para el caso de los controles CRED de 1 mes (29 días) a 11 meses 29 días (364 días) de edad, se considera un intervalo mensual entre control y control (para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control)
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_mes2',N'U') is not null drop table #cred_mes2;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, min(b.fecha_atencion) fecha_cred_mes2 
				, num_cred_mes2=1
into #cred_mes2 
from #cred_mes1 a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381')												-- Código de control CRED Mensual.
and ( datediff(dd,fecha_nac,fecha_atencion) between 60 and 89 )						-- El Segundo control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,a.fecha_cred_mes1,b.fecha_atencion)>=28								-- para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control
and b.fecha_atencion<=a.fecha_final												
group by a.num_doc , a.fecha_nac , a.seguro
		, a.ubigeo, a.fecha_final


/* 2.2.3 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. 
// Para el caso de los controles CRED de 1 mes (29 días) a 11 meses 29 días (364 días) de edad, se considera un intervalo mensual entre control y control (para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control)
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_mes3',N'U') is not null drop table #cred_mes3;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, min(b.fecha_atencion) fecha_cred_mes3 
				, num_cred_mes3=1
into #cred_mes3 
from #cred_mes2 a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381')												-- Código de control CRED Mensual.
and ( datediff(dd,fecha_nac,fecha_atencion) between 90 and 119 )					-- El Tercer control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,a.fecha_cred_mes2,b.fecha_atencion)>=28								-- para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control
and b.fecha_atencion<=a.fecha_final												
group by a.num_doc , a.fecha_nac , a.seguro
		, a.ubigeo, a.fecha_final


/* 2.2.4 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. 
// Para el caso de los controles CRED de 1 mes (29 días) a 11 meses 29 días (364 días) de edad, se considera un intervalo mensual entre control y control (para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control)
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_mes4',N'U') is not null drop table #cred_mes4;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, min(b.fecha_atencion) fecha_cred_mes4 
				, num_cred_mes4=1
into #cred_mes4 
from #cred_mes3 a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381')												-- Código de control CRED Mensual.
and ( datediff(dd,fecha_nac,fecha_atencion) between 120 and 149 )					-- El Cuarto control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,a.fecha_cred_mes3,b.fecha_atencion)>=28								-- para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control
and b.fecha_atencion<=a.fecha_final												
group by a.num_doc , a.fecha_nac , a.seguro
		, a.ubigeo, a.fecha_final


/* 2.2.5 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. 
// Para el caso de los controles CRED de 1 mes (29 días) a 11 meses 29 días (364 días) de edad, se considera un intervalo mensual entre control y control (para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control)
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_mes5',N'U') is not null drop table #cred_mes5;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, min(b.fecha_atencion) fecha_cred_mes5 
				, num_cred_mes5=1
into #cred_mes5 
from #cred_mes4 a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381')												-- Código de control CRED Mensual.
and ( datediff(dd,fecha_nac,fecha_atencion) between 150 and 179 )					-- El quinto control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,a.fecha_cred_mes4,b.fecha_atencion)>=28								-- para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control
and b.fecha_atencion<=a.fecha_final												
group by a.num_doc , a.fecha_nac , a.seguro
		, a.ubigeo, a.fecha_final


/* 2.2.6 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. 
// Para el caso de los controles CRED de 1 mes (29 días) a 11 meses 29 días (364 días) de edad, se considera un intervalo mensual entre control y control (para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control)
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_mes6',N'U') is not null drop table #cred_mes6;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, min(b.fecha_atencion) fecha_cred_mes6 
				, num_cred_mes6=1
into #cred_mes6 
from #cred_mes5 a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381')												-- Código de control CRED Mensual.
and ( datediff(dd,fecha_nac,fecha_atencion) between 180 and 209 )					-- El sexto control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,a.fecha_cred_mes5,b.fecha_atencion)>=28								-- para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control
and b.fecha_atencion<=a.fecha_final												
group by a.num_doc , a.fecha_nac , a.seguro
		, a.ubigeo, a.fecha_final


/* 2.2.7 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. 
// Para el caso de los controles CRED de 1 mes (29 días) a 11 meses 29 días (364 días) de edad, se considera un intervalo mensual entre control y control (para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control)
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_mes7',N'U') is not null drop table #cred_mes7;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, min(b.fecha_atencion) fecha_cred_mes7 
				, num_cred_mes7=1
into #cred_mes7 
from #cred_mes6 a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381')												-- Código de control CRED Mensual.
and ( datediff(dd,fecha_nac,fecha_atencion) between 210 and 239 )					-- El septimo control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,a.fecha_cred_mes6,b.fecha_atencion)>=28								-- para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control
and b.fecha_atencion<=a.fecha_final												
group by a.num_doc , a.fecha_nac , a.seguro
		, a.ubigeo, a.fecha_final


/* 2.2.8 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. 
// Para el caso de los controles CRED de 1 mes (29 días) a 11 meses 29 días (364 días) de edad, se considera un intervalo mensual entre control y control (para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control)
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_mes8',N'U') is not null drop table #cred_mes8;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, min(b.fecha_atencion) fecha_cred_mes8 
				, num_cred_mes8=1
into #cred_mes8 
from #cred_mes7 a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381')												-- Código de control CRED Mensual.
and ( datediff(dd,fecha_nac,fecha_atencion) between 240 and 269 )					-- El octavo control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,a.fecha_cred_mes7,b.fecha_atencion)>=28								-- para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control
and b.fecha_atencion<=a.fecha_final												
group by a.num_doc , a.fecha_nac , a.seguro
		, a.ubigeo, a.fecha_final


/* 2.2.9 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. 
// Para el caso de los controles CRED de 1 mes (29 días) a 11 meses 29 días (364 días) de edad, se considera un intervalo mensual entre control y control (para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control)
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_mes9',N'U') is not null drop table #cred_mes9;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, min(b.fecha_atencion) fecha_cred_mes9 
				, num_cred_mes9=1
into #cred_mes9 
from #cred_mes8 a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381')												-- Código de control CRED Mensual.
and ( datediff(dd,fecha_nac,fecha_atencion) between 270 and 299 )					-- El noveno control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,a.fecha_cred_mes8,b.fecha_atencion)>=28								-- para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control
and b.fecha_atencion<=a.fecha_final												
group by a.num_doc , a.fecha_nac , a.seguro
		, a.ubigeo, a.fecha_final


/* 2.2.10 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. 
// Para el caso de los controles CRED de 1 mes (29 días) a 11 meses 29 días (364 días) de edad, se considera un intervalo mensual entre control y control (para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control)
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_mes10',N'U') is not null drop table #cred_mes10;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, min(b.fecha_atencion) fecha_cred_mes10 
				, num_cred_mes10=1
into #cred_mes10 
from #cred_mes9 a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381')												-- Código de control CRED Mensual.
and ( datediff(dd,fecha_nac,fecha_atencion) between 300 and 329 )					-- El decimo control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,a.fecha_cred_mes9,b.fecha_atencion)>=28								-- para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control
and b.fecha_atencion<=a.fecha_final												
group by a.num_doc , a.fecha_nac , a.seguro
		, a.ubigeo, a.fecha_final


/* 2.2.11 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) de acuerdo a su edad identificado con código CIE-10: Z001 o CPMS 99381*
		, en la misma atención. 
// Para el caso de los controles CRED de 1 mes (29 días) a 11 meses 29 días (364 días) de edad, se considera un intervalo mensual entre control y control (para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control)
// Se contabilizará los controles CRED, independientemente del LAB registrado. */
if Object_id(N'tempdb..#cred_mes11',N'U') is not null drop table #cred_mes11;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, min(b.fecha_atencion) fecha_cred_mes11 
				, num_cred_mes11=1
into #cred_mes11 
from #cred_mes10 a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('Z001','99381')												-- Código de control CRED Mensual.
and ( datediff(dd,fecha_nac,fecha_atencion) between 330 and 364 )					-- El Onceavo control es hasta un dia antes del inicio del proximo rango de edad edad.
and	datediff(dd,a.fecha_cred_mes10,b.fecha_atencion)>=28							-- para efectos de búsqueda de datos, se considera un mínimo de 28 días entre control y control
and b.fecha_atencion<=a.fecha_final												
group by a.num_doc , a.fecha_nac , a.seguro
		, a.ubigeo, a.fecha_final


/* 2.3.12 - Niñas y niños menores del denominador que han recibido controles de crecimiento y desarrollo (CRED) Mensual de acuerdo a su edad. */
if Object_id(N'tempdb..#num_CRED_mes',N'U') is not null drop table #num_CRED_mes;
select  *
, case when edad_dias<=59 then 1
		when (edad_dias between 60 and 89)	 and num_cred_mes1=1 then 1
		when (edad_dias between 90 and 119)	 and (num_cred_mes1+num_cred_mes2)=2 then 1
		when (edad_dias between 120 and 149) and (num_cred_mes1+num_cred_mes2+num_cred_mes3)=3 then 1
		when (edad_dias between 150 and 179) and (num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4)=4 then 1
		when (edad_dias between 180 and 209) and (num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4+num_cred_mes5)=5 then 1
		when (edad_dias between 210 and 239) and (num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4+num_cred_mes5+num_cred_mes6)=6 then 1
		when (edad_dias between 240 and 269) and (num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4+num_cred_mes5+num_cred_mes6+num_cred_mes7)=7 then 1
		when (edad_dias between 270 and 299) and (num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4+num_cred_mes5+num_cred_mes6+num_cred_mes7+num_cred_mes8)=8 then 1
		when (edad_dias between 300 and 329) and (num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4+num_cred_mes5+num_cred_mes6+num_cred_mes7+num_cred_mes8+num_cred_mes9)=9 then 1
		when (edad_dias between 330 and 363) and (num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4+num_cred_mes5+num_cred_mes6+num_cred_mes7+num_cred_mes8+num_cred_mes9+num_cred_mes10)=10 then 1
		when edad_dias>=364 and (num_cred_mes1+num_cred_mes2+num_cred_mes3+num_cred_mes4+num_cred_mes5+num_cred_mes6+num_cred_mes7+num_cred_mes8+num_cred_mes9+num_cred_mes10+num_cred_mes11)=11 then 1 else 0 end num_cred_mensual -- CRED Mensual de acuerdo a su edad.
into #num_CRED_mes
from (
		select a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
		, min(b1.fecha_cred_mes1)	fecha_cred_mes1	,	max(isnull(b1.num_cred_mes1,0))		num_cred_mes1
		, min(b2.fecha_cred_mes2)	fecha_cred_mes2	,	max(isnull(b2.num_cred_mes2,0))		num_cred_mes2
		, min(b3.fecha_cred_mes3)	fecha_cred_mes3	,	max(isnull(b3.num_cred_mes3,0))		num_cred_mes3
		, min(b4.fecha_cred_mes4)	fecha_cred_mes4	,	max(isnull(b4.num_cred_mes4,0))		num_cred_mes4
		, min(b5.fecha_cred_mes5)	fecha_cred_mes5	,	max(isnull(b5.num_cred_mes5,0))		num_cred_mes5
		, min(b6.fecha_cred_mes6)	fecha_cred_mes6	,	max(isnull(b6.num_cred_mes6,0))		num_cred_mes6
		, min(b7.fecha_cred_mes7)	fecha_cred_mes7	,	max(isnull(b7.num_cred_mes7,0))		num_cred_mes7
		, min(b8.fecha_cred_mes8)	fecha_cred_mes8	,	max(isnull(b8.num_cred_mes8,0))		num_cred_mes8
		, min(b9.fecha_cred_mes9)	fecha_cred_mes9	,	max(isnull(b9.num_cred_mes9,0))		num_cred_mes9
		, min(b10.fecha_cred_mes10)	fecha_cred_mes10,	max(isnull(b10.num_cred_mes10,0))	num_cred_mes10
		, min(b11.fecha_cred_mes11)	fecha_cred_mes11,	max(isnull(b11.num_cred_mes11,0))	num_cred_mes11
		from #denominador	a
		left join #cred_mes1	b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo			-- Trama de 1° Cred Mensual.
		left join #cred_mes2	b2	on a.num_doc=b2.num_doc and a.seguro=b2.seguro and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo			-- Trama de 2° Cred Mensual.
		left join #cred_mes3	b3	on a.num_doc=b3.num_doc and a.seguro=b3.seguro and a.fecha_nac=b3.fecha_nac and a.ubigeo=b3.ubigeo			-- Trama de 3° Cred Mensual.
		left join #cred_mes4	b4	on a.num_doc=b4.num_doc and a.seguro=b4.seguro and a.fecha_nac=b4.fecha_nac and a.ubigeo=b4.ubigeo			-- Trama de 4° Cred Mensual.
		left join #cred_mes5	b5	on a.num_doc=b5.num_doc and a.seguro=b5.seguro and a.fecha_nac=b5.fecha_nac and a.ubigeo=b5.ubigeo			-- Trama de 5° Cred Mensual.
		left join #cred_mes6	b6	on a.num_doc=b6.num_doc and a.seguro=b6.seguro and a.fecha_nac=b6.fecha_nac and a.ubigeo=b6.ubigeo			-- Trama de 6° Cred Mensual.
		left join #cred_mes7	b7	on a.num_doc=b7.num_doc and a.seguro=b7.seguro and a.fecha_nac=b7.fecha_nac and a.ubigeo=b7.ubigeo			-- Trama de 7° Cred Mensual.
		left join #cred_mes8	b8	on a.num_doc=b8.num_doc and a.seguro=b8.seguro and a.fecha_nac=b8.fecha_nac and a.ubigeo=b8.ubigeo			-- Trama de 8° Cred Mensual.
		left join #cred_mes9	b9	on a.num_doc=b9.num_doc and a.seguro=b9.seguro and a.fecha_nac=b9.fecha_nac and a.ubigeo=b9.ubigeo			-- Trama de 9° Cred Mensual.
		left join #cred_mes10	b10	on a.num_doc=b10.num_doc and a.seguro=b10.seguro and a.fecha_nac=b10.fecha_nac and a.ubigeo=b10.ubigeo		-- Trama de 10° Cred Mensual.
		left join #cred_mes11	b11	on a.num_doc=b11.num_doc and a.seguro=b11.seguro and a.fecha_nac=b11.fecha_nac and a.ubigeo=b11.ubigeo		-- Trama de 11° Cred Mensual.
		group by a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
) as t 


/* ===================================================================================================================================
									2.3  Vacuna Antineumocócica

-------------------------------------------------------------------------------------------------
 Dosis Periodos de búsqueda de Vacuna Rango de edad para calificar la condición (días) CUMPLE
-------------------------------------------------------------------------------------------------
N°		Dosis			Periodo de busqueda			Rango de busqueda		Cumple		
1														0-119				Todos
2		1era dosis		Niños >=55 y <=119 día			120-147				1
3		2da dosis		>=1era dosis +28 dias o			148-364				1+1
						<= 1era dosis+70 días 		

====================================================================================================================================== */

/* 2.3.1 - Niñas y niños menores del denominador que han recibido inmunizaciones con vacuna antineumocócica, según edad (esquema de vacunación): Identificado con código "90670" 
		// Primera vacuna antineumocócica  */
if Object_id(N'tempdb..#vac_antineumococica1',N'U') is not null drop table #vac_antineumococica1;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_vac_antineumococica1 
				, num_vac_antineumococica1=1
into #vac_antineumococica1 
from #denominador a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item='90670'															-- Código de Vacuna Antineumocócica.
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 55 and 119)					-- Busqueda de 1° Dosis entre los 55 y 119 dias de edad.
and b.fecha_atencion<=a.fecha_final		


/* 2.3.2 - Niñas y niños menores del denominador que han recibido inmunizaciones con vacuna antineumocócica, según edad (esquema de vacunación): Identificado con código "90670" 
		// Segunda vacuna antineumocócica  */
if Object_id(N'tempdb..#vac_antineumococica2',N'U') is not null drop table #vac_antineumococica2;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_vac_antineumococica2 
				, num_vac_antineumococica2=1
into #vac_antineumococica2 
from #vac_antineumococica1  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item='90670'																-- Código de Vacuna Antineumocócica.
and (datediff(dd,a.fecha_vac_antineumococica1,b.fecha_atencion) between 28 and 70)		-- Busqueda de 2° Dosis entre 28 y 70 dias despues de la 1° dosis.
and b.fecha_atencion<=a.fecha_final	


/* 2.3.3 - Niñas y niños menores del denominador que han recibido vacuna Antineumococica de acuerdo a su edad. */
if Object_id(N'tempdb..#num_vac_antineumococica',N'U') is not null drop table #num_vac_antineumococica;
select  *
, case	when edad_dias<=119	then 1
		when (edad_dias between 120 and 147) and num_vac_antineumococica1=1 then 1
		when edad_dias>=148 and num_vac_antineumococica1=1 and num_vac_antineumococica2=1 then 1 else 0 end num_vac_antineumococica
into #num_vac_antineumococica
from (
		select a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
		, min(b1.fecha_vac_antineumococica1)	fecha_vac_antineumococica1	, max(isnull(b1.num_vac_antineumococica1,0))	num_vac_antineumococica1
		, min(b2.fecha_vac_antineumococica2)	fecha_vac_antineumococica2	, max(isnull(b2.num_vac_antineumococica2,0))	num_vac_antineumococica2
		from #denominador	a
		left join #vac_antineumococica1	b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Trama 1° Dosis de vacuna antineumococica
		left join #vac_antineumococica2	b2	on a.num_doc=b2.num_doc and a.seguro=b2.seguro and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo				-- Trama 2° Dosis de vacuna antineumococica
		group by a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
) as t 


/* ===================================================================================================================================
									2.4  Vacuna Rotavirus

-------------------------------------------------------------------------------------------------
 Dosis Periodos de búsqueda de Vacuna Rango de edad para calificar la condición (días) CUMPLE
-------------------------------------------------------------------------------------------------
N°		Dosis			Periodo de busqueda			Rango de busqueda		Cumple		
1														0-180				Todos
2		1era dosis		Niños >=55 y <=180 día			181-208				1
3		2da dosis		>=1era dosis +28 dias o			209-364				1+1
						<= 240 dias (edad del niño)
						
====================================================================================================================================== */

/* 2.4.1 - Niñas y niños menores del denominador que han recibido inmunizaciones con vacuna rotavirus, según edad (esquema de vacunación): Identificado con código "90681" 
		// Primera vacuna rotavirus  */
if Object_id(N'tempdb..#vac_rotavirus1',N'U') is not null drop table #vac_rotavirus1;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_vac_rotavirus1 
				, num_vac_rotavirus1=1
into #vac_rotavirus1 
from #denominador a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item='90681'															-- Código de Vacuna rotavirus.
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 55 and 180)					-- Busqueda de 1° Dosis entre los 55 y 180 dias de edad.
and b.fecha_atencion<=a.fecha_final		


/* 2.4.2 - Niñas y niños menores del denominador que han recibido inmunizaciones con vacuna rotavirus, según edad (esquema de vacunación): Identificado con código "90681" 
		// Segunda vacuna rotavirus  */
if Object_id(N'tempdb..#vac_rotavirus2',N'U') is not null drop table #vac_rotavirus2;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_vac_rotavirus2 
				, num_vac_rotavirus2=1
into #vac_rotavirus2 
from #vac_rotavirus1 a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item='90681'															-- Código de Vacuna rotavirus.
and datediff(dd,a.fecha_vac_rotavirus1,b.fecha_atencion)>=28				
and b.fecha_atencion<=dateadd(dd,240,a.fecha_nac)									-- Busqueda de 2° Dosis entre 28 dias despues de la 1° dosis hasta los 240 dias de edad.
and b.fecha_atencion<=a.fecha_final		


/* 2.4.3 - Niñas y niños menores del denominador que han recibido vacuna Antineumococica de acuerdo a su edad. */
if Object_id(N'tempdb..#num_vac_rotavirus',N'U') is not null drop table #num_vac_rotavirus;
select  *
, case	when edad_dias<=180	then 1
		when (edad_dias between 181 and 208) and num_vac_rotavirus1=1 then 1
		when edad_dias>=209 and num_vac_rotavirus1=1 and num_vac_rotavirus2=1 then 1 else 0 end num_vac_rotavirus
into #num_vac_rotavirus
from (
		select a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
		, min(b1.fecha_vac_rotavirus1)	fecha_vac_rotavirus1	, max(isnull(b1.num_vac_rotavirus1,0))	num_vac_rotavirus1
		, min(b2.fecha_vac_rotavirus2)	fecha_vac_rotavirus2	, max(isnull(b2.num_vac_rotavirus2,0))	num_vac_rotavirus2
		from #denominador	a
		left join #vac_rotavirus1	b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Trama 1° Dosis de vacuna rotavirus
		left join #vac_rotavirus2	b2	on a.num_doc=b2.num_doc and a.seguro=b2.seguro and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo				-- Trama 2° Dosis de vacuna rotavirus
		group by a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
) as t 


/* ===================================================================================================================================
									2.5  Vacuna Antipolio

-------------------------------------------------------------------------------------------------
 Dosis Periodos de búsqueda de Vacuna Rango de edad para calificar la condición (días) CUMPLE
-------------------------------------------------------------------------------------------------
N°		Dosis			Periodo de busqueda			Rango de busqueda		Cumple		
1														0-119				Todos
2		1era dosis		Niños >=55 y <=119 días			120-147				1
3		2da dosis		>= 1era dosis + 28 dias o		148-217				1+1
						<= 1era dosis + 70 días 
4		3ra dosis		>= 2da dosis + 28 dias o		218-364				1+1+1
						<= 2da dosis + 70 días 
												
====================================================================================================================================== */

/* 2.5.1 - Niñas y niños menores del denominador que han recibido inmunizaciones con vacuna Antipolio, según edad (esquema de vacunación): Identificado con código "90712 o 90713"
		// Primera vacuna antipolio  */
if Object_id(N'tempdb..#vac_antipolio1',N'U') is not null drop table #vac_antipolio1;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_vac_antipolio1 
				, num_vac_antipolio1=1
into #vac_antipolio1 
from #denominador a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('90712','90713')												-- Código de Vacuna Antipolio.
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 55 and 119)					-- Busqueda de 1° Dosis entre los 55 y 119 dias de edad.
and b.fecha_atencion<=a.fecha_final		


/* 2.5.2 - Niñas y niños menores del denominador que han recibido inmunizaciones con vacuna Antipolio, según edad (esquema de vacunación): Identificado con código "90712 o 90713"
		// Segunda vacuna antipolio   */
if Object_id(N'tempdb..#vac_antipolio2',N'U') is not null drop table #vac_antipolio2;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_vac_antipolio2 
				, num_vac_antipolio2=1
into #vac_antipolio2 
from #vac_antipolio1  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('90712','90713')												-- Código de Vacuna Antipolio.
and (datediff(dd,a.fecha_vac_antipolio1,b.fecha_atencion) between 28 and 70)		-- Busqueda de 2° Dosis entre 28 y 70 dias despues de la 1° dosis.
and b.fecha_atencion<=a.fecha_final	


/* 2.5.3 - Niñas y niños menores del denominador que han recibido inmunizaciones con vacuna Antipolio, según edad (esquema de vacunación): Identificado con código "90712 o 90713"
		// Tercera vacuna antipolio   */
if Object_id(N'tempdb..#vac_antipolio3',N'U') is not null drop table #vac_antipolio3;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_vac_antipolio3 
				, num_vac_antipolio3=1
into #vac_antipolio3 
from #vac_antipolio2  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('90712','90713')												-- Código de Vacuna Antipolio.
and (datediff(dd,a.fecha_vac_antipolio2,b.fecha_atencion) between 28 and 70)		-- Busqueda de 3° Dosis entre 28 y 70 dias despues de la 2° dosis.
and b.fecha_atencion<=a.fecha_final	


/* 2.5.4 - Niñas y niños menores del denominador que han recibido vacuna Antipolio de acuerdo a su edad. */
if Object_id(N'tempdb..#num_vac_antipolio',N'U') is not null drop table #num_vac_antipolio;
select  *
, case	when edad_dias<=119	then 1
		when (edad_dias between 120 and 147) and num_vac_antipolio1=1 then 1
		when (edad_dias between 148 and 217) and num_vac_antipolio1=1 and num_vac_antipolio2=1 then 1
		when edad_dias>=218 and num_vac_antipolio1=1 and num_vac_antipolio2=1 and num_vac_antipolio3=1 then 1 else 0 end num_vac_antipolio
into #num_vac_antipolio
from (
		select a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
		, min(b1.fecha_vac_antipolio1)	fecha_vac_antipolio1	, max(isnull(b1.num_vac_antipolio1,0))	num_vac_antipolio1
		, min(b2.fecha_vac_antipolio2)	fecha_vac_antipolio2	, max(isnull(b2.num_vac_antipolio2,0))	num_vac_antipolio2
		, min(b3.fecha_vac_antipolio3)	fecha_vac_antipolio3	, max(isnull(b3.num_vac_antipolio3,0))	num_vac_antipolio3
		from #denominador	a
		left join #vac_antipolio1	b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Trama 1° Dosis de vacuna antipolio
		left join #vac_antipolio2	b2	on a.num_doc=b2.num_doc and a.seguro=b2.seguro and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo				-- Trama 2° Dosis de vacuna antipolio
		left join #vac_antipolio3	b3	on a.num_doc=b3.num_doc and a.seguro=b3.seguro and a.fecha_nac=b3.fecha_nac and a.ubigeo=b3.ubigeo				-- Trama 3° Dosis de vacuna antipolio		
		group by a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
) as t 


/* ===================================================================================================================================
									2.6  Vacuna Pentavalente

-------------------------------------------------------------------------------------------------
 Dosis Periodos de búsqueda de Vacuna Rango de edad para calificar la condición (días) CUMPLE
-------------------------------------------------------------------------------------------------
N°		Dosis			Periodo de busqueda			Rango de busqueda		Cumple		
1														0-119				Todos
2		1era dosis		Niños >=55 y <=119 días			120-147				1
3		2da dosis		>= 1era dosis + 28 dias o		148-217				1+1
						<= 1era dosis + 70 días 
4		3ra dosis		>= 2da dosis + 28 dias o		218-364				1+1+1
						<= 2da dosis + 70 días 
												
====================================================================================================================================== */

/* 2.6.1 - Niñas y niños menores del denominador que han recibido inmunizaciones con vacuna Pentavalente, según edad (esquema de vacunación): Identificado con código "90723 o 90722"
		// Primera vacuna pentavalente  */
if Object_id(N'tempdb..#vac_pentavalente1',N'U') is not null drop table #vac_pentavalente1;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_vac_pentavalente1 
				, num_vac_pentavalente1=1
into #vac_pentavalente1 
from #denominador a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('90723','90722')												-- Código de Vacuna Pentavalente.
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 55 and 119)					-- Busqueda de 1° Dosis entre los 55 y 119 dias de edad.
and b.fecha_atencion<=a.fecha_final		


/* 2.6.2 - Niñas y niños menores del denominador que han recibido inmunizaciones con vacuna Pentavalente, según edad (esquema de vacunación): Identificado con código "90723 o 90722"
		// Segunda vacuna pentavalente    */
if Object_id(N'tempdb..#vac_pentavalente2',N'U') is not null drop table #vac_pentavalente2;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_vac_pentavalente2 
				, num_vac_pentavalente2=1
into #vac_pentavalente2 
from #vac_pentavalente1  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('90723','90722')												-- Código de Vacuna Pentavalente.
and (datediff(dd,a.fecha_vac_pentavalente1,b.fecha_atencion) between 28 and 70)		-- Busqueda de 2° Dosis entre 28 y 70 dias despues de la 1° dosis.
and b.fecha_atencion<=a.fecha_final	


/* 2.6.3 - Niñas y niños menores del denominador que han recibido inmunizaciones con vacuna Pentavalente, según edad (esquema de vacunación): Identificado con código "90723 o 90722"
		// Tercera vacuna antipolio   */
if Object_id(N'tempdb..#vac_pentavalente3',N'U') is not null drop table #vac_pentavalente3;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_vac_pentavalente3 
				, num_vac_pentavalente3=1
into #vac_pentavalente3 
from #vac_pentavalente2  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('90723','90722')												-- Código de Vacuna Pentavalente.
and (datediff(dd,a.fecha_vac_pentavalente2,b.fecha_atencion) between 28 and 70)		-- Busqueda de 3° Dosis entre 28 y 70 dias despues de la 2° dosis.
and b.fecha_atencion<=a.fecha_final	


/* 2.6.4 - Niñas y niños menores del denominador que han recibido vacuna Pentavalente de acuerdo a su edad. */
if Object_id(N'tempdb..#num_vac_pentavalente',N'U') is not null drop table #num_vac_pentavalente;
select  *
, case	when edad_dias<=119	then 1
		when (edad_dias between 120 and 147) and num_vac_pentavalente1=1 then 1
		when (edad_dias between 148 and 217) and num_vac_pentavalente1=1 and num_vac_pentavalente2=1 then 1
		when edad_dias>=218 and num_vac_pentavalente1=1 and num_vac_pentavalente2=1 and num_vac_pentavalente3=1 then 1 else 0 end num_vac_pentavalente
into #num_vac_pentavalente
from (
		select a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
		, min(b1.fecha_vac_pentavalente1)	fecha_vac_pentavalente1	, max(isnull(b1.num_vac_pentavalente1,0))	num_vac_pentavalente1
		, min(b2.fecha_vac_pentavalente2)	fecha_vac_pentavalente2	, max(isnull(b2.num_vac_pentavalente2,0))	num_vac_pentavalente2
		, min(b3.fecha_vac_pentavalente3)	fecha_vac_pentavalente3	, max(isnull(b3.num_vac_pentavalente3,0))	num_vac_pentavalente3
		from #denominador	a
		left join #vac_pentavalente1	b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Trama 1° Dosis de vacuna pentavalente
		left join #vac_pentavalente2	b2	on a.num_doc=b2.num_doc and a.seguro=b2.seguro and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo				-- Trama 2° Dosis de vacuna pentavalente
		left join #vac_pentavalente3	b3	on a.num_doc=b3.num_doc and a.seguro=b3.seguro and a.fecha_nac=b3.fecha_nac and a.ubigeo=b3.ubigeo				-- Trama 3° Dosis de vacuna pentavalente		
		group by a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
) as t 


/* ===================================================================================================================================
									2.7 Suplementación - Esquema 4 meses

-------------------------------------------------------------------------------------------------
Número acumulado de meses. Suplementación preventiva- Esquema 4 meses: Para frascos de hierro
-------------------------------------------------------------------------------------------------
N°		Rango Anterior-Posterior		Rango Para Calificar	Numero Acumuluado	Cumple (N° Frascos)
1												0-179				Todos				Todos
2			110-179								180-364				>=1					1
												
====================================================================================================================================== */

/* 2.7.1 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 4 meses.
	// Registros válidos para suplementación con código 99199.17 (Suplementación de sulfato ferroso)
	// Para la suplementación preventiva, cumple con un frasco de hierro, según edad.	
	// Una entrega de frasco de Hierro.
*/
if Object_id(N'tempdb..#Esq4m_Sup_E1',N'U') is not null drop table #Esq4m_Sup_E1;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_Esq4m_Sup_E1
				, num_Esq4m_Sup_E1=1
into #Esq4m_Sup_E1 
from #denominador  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item='99199.17'													-- Código de Suplementación.
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 110 and 179)			-- Busqueda de suplementación - Esquema 4 meses entre los 110 y 179 dias de edad. 
and b.fecha_atencion<=a.fecha_final	


/* 2.7.2 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 4 meses. */
if Object_id(N'tempdb..#num_Esq4m',N'U') is not null drop table #num_Esq4m;
select  *
, case	when edad_dias<=179	then 1
		when edad_dias>=180 and num_Esq4m_Sup_E1=1 then 1 else 0 end num_Esq4m
into #num_Esq4m
from (
		select a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
		, min(b1.fecha_Esq4m_Sup_E1) fecha_Esq4m_Sup_E1,	max(isnull(b1.num_Esq4m_Sup_E1,0))	num_Esq4m_Sup_E1
		from #denominador	a
		left join #Esq4m_Sup_E1  b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Trama 1° Entrega de Suplementacion preventiva - Esquema 4 meses.
		group by a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
) as t 


/* ===================================================================================================================================
									2.8.1 Tratamiento - Esquema 6 meses

-------------------------------------------------------------------------------------------------
	Número acumulado de meses.	Tratamiento - Esquema 6 meses: Para frascos de hierro
-------------------------------------------------------------------------------------------------
N°		Rango Anterior-Posterior		Rango Para Calificar	Numero Acumuluado	Cumple (N° Frascos)
1												0-239				Todos				Todos
2			170-239								240-299				>=1					1	
3			240-299								300-363				>=2					1+1
4			300-364								364					>=3					1+1+1
====================================================================================================================================== */

/* 2.8.1.1 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses.
	// Registros válidos para tratamiento de anemia con hierro: D500, D508, D509, D649 o D539 + CPMS 99199.17 o 99199.11.
	// Para el tratamiento, cumple con al menos tres frascos de hierro, según edad.
	// Primera entrega de tratamiento.
*/
if Object_id(N'tempdb..#Esq6m_Trat_E1',N'U') is not null drop table #Esq6m_Trat_E1;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_Esq6m_Trat_E1
				, num_Esq6m_Trat_E1=1
into #Esq6m_Trat_E1 
from #denominador  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('99199.17','99199.11')																	-- Código de tratamiento
and b.id_cita in (select distinct id_cita from #his_minsa
				where cod_item in ('D500','D508','D509','D649','D539') and tipo_dx in ('D','R') )			-- Código DX de anemia
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 170 and 239)											-- Busqueda de tratamiento - Esquema 6 meses entre los 170 y 239 dias de edad. 
and b.fecha_atencion<=a.fecha_final	


/* 2.8.1.2 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses.
	// Registros válidos para tratamiento de anemia con hierro: D500, D508, D509, D649 o D539 + CPMS 99199.17 o 99199.11.
	// Para el tratamiento, cumple con al menos tres frascos de hierro, según edad.
	// Segunda entrega de tratamiento.
*/
if Object_id(N'tempdb..#Esq6m_Trat_E2',N'U') is not null drop table #Esq6m_Trat_E2;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_Esq6m_Trat_E2
				, num_Esq6m_Trat_E2=1
into #Esq6m_Trat_E2 
from #denominador  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('99199.17','99199.11')																	-- Código de tratamiento
and b.id_cita in (select distinct id_cita from #his_minsa
				where cod_item in ('D500','D508','D509','D649','D539') and tipo_dx in ('D','R') )			-- Código DX de anemia
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 240 and 299)											-- Busqueda de tratamiento - Esquema 6 meses entre los 240 y 299 dias de edad. 
and b.fecha_atencion<=a.fecha_final	


/* 2.8.1.3 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses.
	// Registros válidos para tratamiento de anemia con hierro: D500, D508, D509, D649 o D539 + CPMS 99199.17 o 99199.11.
	// Para el tratamiento, cumple con al menos tres frascos de hierro, según edad.
	// Tercra entrega de tratamiento.
*/
if Object_id(N'tempdb..#Esq6m_Trat_E3',N'U') is not null drop table #Esq6m_Trat_E3;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_Esq6m_Trat_E3
				, num_Esq6m_Trat_E3=1
into #Esq6m_Trat_E3 
from #denominador  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('99199.17','99199.11')																	-- Código de tratamiento
and b.id_cita in (select distinct id_cita from #his_minsa
				where cod_item in ('D500','D508','D509','D649','D539') and tipo_dx in ('D','R') )			-- Código DX de anemia
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 300 and 364)											-- Busqueda de tratamiento - Esquema 6 meses entre los 300 y 364 dias de edad. 
and b.fecha_atencion<=a.fecha_final	


/* 2.8.1.4 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses [Tratamiento]. */
if Object_id(N'tempdb..#num_Esq6m_Trat',N'U') is not null drop table #num_Esq6m_Trat;
select  *
, case	when edad_dias<=239	then 1
		when (edad_dias between 240 and 299) and num_Esq6m_Trat_E1=1 then 1 
		when (edad_dias between 300 and 363) and num_Esq6m_Trat_E1=1 and num_Esq6m_Trat_E2=1 then 1 
		when edad_dias=364 and num_Esq6m_Trat_E1=1 and num_Esq6m_Trat_E2=1 and num_Esq6m_Trat_E3=1 then 1 else 0 end num_Esq6m_Trat
into #num_Esq6m_Trat
from (
		select a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
		, min(b1.fecha_Esq6m_Trat_E1)	fecha_Esq6m_Trat_E1	,	max(isnull(b1.num_Esq6m_Trat_E1,0))	num_Esq6m_Trat_E1
		, min(b2.fecha_Esq6m_Trat_E2)	fecha_Esq6m_Trat_E2	,	max(isnull(b2.num_Esq6m_Trat_E2,0))	num_Esq6m_Trat_E2
		, min(b3.fecha_Esq6m_Trat_E3)	fecha_Esq6m_Trat_E3	,	max(isnull(b3.num_Esq6m_Trat_E3,0))	num_Esq6m_Trat_E3
		from #denominador	a
		left join #Esq6m_Trat_E1  b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Trama 1° Entrega de tratamiento - Esquema 6 meses.
		left join #Esq6m_Trat_E2  b2	on a.num_doc=b2.num_doc and a.seguro=b2.seguro and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo				-- Trama 2° Entrega de tratamiento - Esquema 6 meses.
		left join #Esq6m_Trat_E3  b3	on a.num_doc=b3.num_doc and a.seguro=b3.seguro and a.fecha_nac=b3.fecha_nac and a.ubigeo=b3.ubigeo				-- Trama 3° Entrega de tratamiento - Esquema 6 meses.
		group by a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
) as t 


/* ===================================================================================================================================
									2.8.2 Suplementación - Esquema 6 meses

-------------------------------------------------------------------------------------------------
	Número acumulado de meses.	Suplementación - Esquema 6 meses: Para frascos de hierro
-------------------------------------------------------------------------------------------------
N°		Rango Anterior-Posterior		Rango Para Calificar	Numero Acumuluado	Cumple (N° Frascos)
1												0-269				Todos				Todos
2			170-269								270-363				>=1					1	
3			270-364								364					>=2					1+1
====================================================================================================================================== */

/* 2.8.2.1 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses.
	// Registros válidos para suplementación preventiva CPMS 99199.17.
	// Para la suplementación, cumple con al menos dos frascos de hierro, según edad.
	// Primera entrega de suplementación.
*/
if Object_id(N'tempdb..#Esq6m_Sup_E1',N'U') is not null drop table #Esq6m_Sup_E1;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_Esq6m_Sup_E1
				, num_Esq6m_Sup_E1=1
into #Esq6m_Sup_E1 
from #denominador  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item='99199.17'																					-- Código de suplementación
and b.id_cita not in (select distinct id_cita from #his_minsa
					where cod_item in ('D500','D508','D509','D649','D539') )								-- Sin Código DX de anemia (Asociado)
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 170 and 269)											-- Busqueda de suplementación - Esquema 6 meses entre los 170 y 269 dias de edad. 
and b.fecha_atencion<=a.fecha_final	


/* 2.8.2.2 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses.
	// Registros válidos para suplementación preventiva CPMS 99199.17.
	// Para la suplementación, cumple con al menos dos frascos de hierro, según edad.
	// Segunda entrega de suplementación.
*/
if Object_id(N'tempdb..#Esq6m_Sup_E2',N'U') is not null drop table #Esq6m_Sup_E2;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_Esq6m_Sup_E2
				, num_Esq6m_Sup_E2=1
into #Esq6m_Sup_E2 
from #denominador  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item='99199.17'																					-- Código de suplementación
and b.id_cita not in (select distinct id_cita from #his_minsa
					where cod_item in ('D500','D508','D509','D649','D539') )								-- Sin Código DX de anemia (Asociado)
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 270 and 364)											-- Busqueda de suplementación - Esquema 6 meses entre los 270 y 364 dias de edad. 
and b.fecha_atencion<=a.fecha_final	


/* 2.8.2.3 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses [suplementación]. */
if Object_id(N'tempdb..#num_Esq6m_sup',N'U') is not null drop table #num_Esq6m_sup;
select  *
, case	when edad_dias<=269	then 1
		when (edad_dias between 270 and 363) and num_Esq6m_Sup_E1=1 then 1 
		when edad_dias=364 and num_Esq6m_Sup_E1=1 and num_Esq6m_Sup_E2=1 then 1 else 0 end num_Esq6m_sup
into #num_Esq6m_sup
from (
		select a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
		, min(b1.fecha_Esq6m_Sup_E1)	fecha_Esq6m_Sup_E1	,	max(isnull(b1.num_Esq6m_Sup_E1,0))	num_Esq6m_Sup_E1
		, min(b2.fecha_Esq6m_Sup_E2)	fecha_Esq6m_Sup_E2	,	max(isnull(b2.num_Esq6m_Sup_E2,0))	num_Esq6m_Sup_E2
		from #denominador	a
		left join #Esq6m_Sup_E1  b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Trama 1° Entrega de suplementación - Esquema 6 meses.
		left join #Esq6m_Sup_E2  b2	on a.num_doc=b2.num_doc and a.seguro=b2.seguro and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo				-- Trama 2° Entrega de suplementación - Esquema 6 meses.
		group by a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
) as t 


/* ===================================================================================================================================
									2.8.3 Multimicronutrientes - Esquema 6 meses

-------------------------------------------------------------------------------------------------------------
	Número acumulado de meses.	Multimicronutrientes - Esquema 6 meses: Para entrega de multimicronutrientes
-------------------------------------------------------------------------------------------------------------
N°		Rango Anterior-Posterior		Rango Para Calificar	Numero Acumuluado	Cumple (N° Frascos)
1												0-209				Todos				Todos
2			170-209								210-239				>=1					1	
3			210-239								240-269				>=2					1+1
4			240-269								270-299				>=3					1+1+1
5			270-299								300-329				>=4					1+1+1+1
6			300-329								330-363				>=5					1+1+1+1+1
7			330-364								364					>=6					1+1+1+1+1+1
====================================================================================================================================== */

/* 2.8.3.1 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses.
	// Registros válidos para multimicronutrientes CPMS 99199.19.
	// Para multimicronutrientes, cumple con al menos 6 entregas, según edad.
	// Primera entrega de multimicronutrientes.
*/
if Object_id(N'tempdb..#Esq6m_multi_E1',N'U') is not null drop table #Esq6m_multi_E1;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_Esq6m_multi_E1
				, num_Esq6m_multi_E1=1
into #Esq6m_multi_E1 
from #denominador  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item='99199.19'																					-- Código de multimicronutrientes
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 170 and 209)											-- Busqueda de multimicronutrientes - Esquema 6 meses entre los 170 y 209 dias de edad. 
and b.fecha_atencion<=a.fecha_final	


/* 2.8.3.2 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses.
	// Registros válidos para multimicronutrientes CPMS 99199.19.
	// Para multimicronutrientes, cumple con al menos 6 entregas, según edad.
	// Segunda entrega de multimicronutrientes.
*/
if Object_id(N'tempdb..#Esq6m_multi_E2',N'U') is not null drop table #Esq6m_multi_E2;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_Esq6m_multi_E2
				, num_Esq6m_multi_E2=1
into #Esq6m_multi_E2 
from #denominador  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item='99199.19'																					-- Código de multimicronutrientes
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 210 and 239)											-- Busqueda de multimicronutrientes - Esquema 6 meses entre los 210 y 239 dias de edad. 
and b.fecha_atencion<=a.fecha_final	


/* 2.8.3.3 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses.
	// Registros válidos para multimicronutrientes CPMS 99199.19.
	// Para multimicronutrientes, cumple con al menos 6 entregas, según edad.
	// Tercera entrega de multimicronutrientes.
*/
if Object_id(N'tempdb..#Esq6m_multi_E3',N'U') is not null drop table #Esq6m_multi_E3;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_Esq6m_multi_E3
				, num_Esq6m_multi_E3=1
into #Esq6m_multi_E3 
from #denominador  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item='99199.19'																					-- Código de multimicronutrientes
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 240 and 269)											-- Busqueda de multimicronutrientes - Esquema 6 meses entre los 240 y 269 dias de edad. 
and b.fecha_atencion<=a.fecha_final	


/* 2.8.3.4 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses.
	// Registros válidos para multimicronutrientes CPMS 99199.19.
	// Para multimicronutrientes, cumple con al menos 6 entregas, según edad.
	// Cuarta entrega de multimicronutrientes.
*/
if Object_id(N'tempdb..#Esq6m_multi_E4',N'U') is not null drop table #Esq6m_multi_E4;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_Esq6m_multi_E4
				, num_Esq6m_multi_E4=1
into #Esq6m_multi_E4 
from #denominador  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item='99199.19'																					-- Código de multimicronutrientes
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 270 and 299)											-- Busqueda de multimicronutrientes - Esquema 6 meses entre los 270 y 299 dias de edad. 
and b.fecha_atencion<=a.fecha_final	


/* 2.8.3.5 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses.
	// Registros válidos para multimicronutrientes CPMS 99199.19.
	// Para multimicronutrientes, cumple con al menos 6 entregas, según edad.
	// Quinta entrega de multimicronutrientes.
*/
if Object_id(N'tempdb..#Esq6m_multi_E5',N'U') is not null drop table #Esq6m_multi_E5;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_Esq6m_multi_E5
				, num_Esq6m_multi_E5=1
into #Esq6m_multi_E5 
from #denominador  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item='99199.19'																					-- Código de multimicronutrientes
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 300 and 329)											-- Busqueda de multimicronutrientes - Esquema 6 meses entre los 300 y 329 dias de edad. 
and b.fecha_atencion<=a.fecha_final	


/* 2.8.3.6 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses.
	// Registros válidos para multimicronutrientes CPMS 99199.19.
	// Para multimicronutrientes, cumple con al menos 6 entregas, según edad.
	// Sexta entrega de multimicronutrientes.
*/
if Object_id(N'tempdb..#Esq6m_multi_E6',N'U') is not null drop table #Esq6m_multi_E6;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_Esq6m_multi_E6
				, num_Esq6m_multi_E6=1
into #Esq6m_multi_E6 
from #denominador  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item='99199.19'																					-- Código de multimicronutrientes
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 330 and 364)											-- Busqueda de multimicronutrientes - Esquema 6 meses entre los 330 y 364 dias de edad. 
and b.fecha_atencion<=a.fecha_final	


/* 2.8.3.7 - Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses [Multimicronutrientes]. */
if Object_id(N'tempdb..#num_Esq6m_Multi',N'U') is not null drop table #num_Esq6m_Multi;
select  *
, case	when edad_dias<=209	then 1
		when (edad_dias between 210 and 239) and num_Esq6m_Multi_E1=1 then 1 
		when (edad_dias between 240 and 269) and num_Esq6m_Multi_E1=1 and num_Esq6m_Multi_E2=1 then 1 
		when (edad_dias between 270 and 299) and num_Esq6m_Multi_E1=1 and num_Esq6m_Multi_E2=1 and num_Esq6m_Multi_E3=1 then 1 
		when (edad_dias between 300 and 329) and num_Esq6m_Multi_E1=1 and num_Esq6m_Multi_E2=1 and num_Esq6m_Multi_E3=1 and num_Esq6m_Multi_E4=1 then 1 
		when (edad_dias between 330 and 363) and num_Esq6m_Multi_E1=1 and num_Esq6m_Multi_E2=1 and num_Esq6m_Multi_E3=1 and num_Esq6m_Multi_E4=1 and num_Esq6m_Multi_E5=1 then 1 
		when edad_dias>=364 and num_Esq6m_Multi_E1=1 and num_Esq6m_Multi_E2=1 and num_Esq6m_Multi_E3=1 and num_Esq6m_Multi_E4=1 and num_Esq6m_Multi_E5=1 and num_Esq6m_Multi_E6=1 then 1 else 0 end num_Esq6m_multi
into #num_Esq6m_Multi
from (
		select a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
		, min(b1.fecha_Esq6m_Multi_E1)	fecha_Esq6m_Multi_E1	,	max(isnull(b1.num_Esq6m_Multi_E1,0))	num_Esq6m_Multi_E1
		, min(b2.fecha_Esq6m_Multi_E2)	fecha_Esq6m_Multi_E2	,	max(isnull(b2.num_Esq6m_Multi_E2,0))	num_Esq6m_Multi_E2
		, min(b3.fecha_Esq6m_Multi_E3)	fecha_Esq6m_Multi_E3	,	max(isnull(b3.num_Esq6m_Multi_E3,0))	num_Esq6m_Multi_E3
		, min(b4.fecha_Esq6m_Multi_E4)	fecha_Esq6m_Multi_E4	,	max(isnull(b4.num_Esq6m_Multi_E4,0))	num_Esq6m_Multi_E4
		, min(b5.fecha_Esq6m_Multi_E5)	fecha_Esq6m_Multi_E5	,	max(isnull(b5.num_Esq6m_Multi_E5,0))	num_Esq6m_Multi_E5
		, min(b6.fecha_Esq6m_multi_E6)	fecha_Esq6m_Multi_E6	,	max(isnull(b6.num_Esq6m_Multi_E6,0))	num_Esq6m_Multi_E6
		from #denominador	a
		left join #Esq6m_Multi_E1  b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Trama 1° Entrega de Multimicronutrientes - Esquema 6 meses.
		left join #Esq6m_Multi_E2  b2	on a.num_doc=b2.num_doc and a.seguro=b2.seguro and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo				-- Trama 2° Entrega de Multimicronutrientes - Esquema 6 meses.
		left join #Esq6m_Multi_E3  b3	on a.num_doc=b3.num_doc and a.seguro=b3.seguro and a.fecha_nac=b3.fecha_nac and a.ubigeo=b3.ubigeo				-- Trama 3° Entrega de Multimicronutrientes - Esquema 6 meses.
		left join #Esq6m_Multi_E4  b4	on a.num_doc=b4.num_doc and a.seguro=b4.seguro and a.fecha_nac=b4.fecha_nac and a.ubigeo=b4.ubigeo				-- Trama 4° Entrega de Multimicronutrientes - Esquema 6 meses.
		left join #Esq6m_Multi_E5  b5	on a.num_doc=b5.num_doc and a.seguro=b5.seguro and a.fecha_nac=b5.fecha_nac and a.ubigeo=b5.ubigeo				-- Trama 5° Entrega de Multimicronutrientes - Esquema 6 meses.
		left join #Esq6m_Multi_E6  b6	on a.num_doc=b6.num_doc and a.seguro=b6.seguro and a.fecha_nac=b6.fecha_nac and a.ubigeo=b6.ubigeo				-- Trama 6° Entrega de Multimicronutrientes - Esquema 6 meses.
		group by a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
) as t 


/* ===================================================================================================================================
									2.8.4 Esquema 6 meses [Suplementación/Tratamiento/Multimicronutrientes]

	Niñas y niños menores del denominador que han recibido gotas o jarabe de hierro o multimicronutrientes, según edad - Esquema 6 meses
====================================================================================================================================== */
if Object_id(N'tempdb..#num_Esq6m',N'U') is not null drop table #num_Esq6m;
select a.num_doc
, a.fecha_nac
, a.seguro
, a.ubigeo
, a.fecha_final
, b1.fecha_Esq6m_Trat_E1	, b1.num_Esq6m_Trat_E1
, b1.fecha_Esq6m_Trat_E2	, b1.num_Esq6m_Trat_E2
, b1.fecha_Esq6m_Trat_E3	, b1.num_Esq6m_Trat_E3
, b1.num_Esq6m_Trat
, b2.fecha_Esq6m_Sup_E1		, b2.num_Esq6m_Sup_E1
, b2.fecha_Esq6m_Sup_E2		, b2.num_Esq6m_Sup_E2
, b2.num_Esq6m_sup
, b3.fecha_Esq6m_Multi_E1	, b3.num_Esq6m_Multi_E1
, b3.fecha_Esq6m_Multi_E2	, b3.num_Esq6m_Multi_E2
, b3.fecha_Esq6m_Multi_E3	, b3.num_Esq6m_Multi_E3
, b3.fecha_Esq6m_Multi_E4	, b3.num_Esq6m_Multi_E4
, b3.fecha_Esq6m_Multi_E5	, b3.num_Esq6m_Multi_E5
, b3.fecha_Esq6m_Multi_E6	, b3.num_Esq6m_Multi_E6
, b3.num_Esq6m_multi
, iif(b1.num_Esq6m_Trat=1 or b2.num_Esq6m_sup=1 or b3.num_Esq6m_multi=1,1,0) num_esq6m														-- Cumple esquema de 6 meses [ Tratamiento/suplementación/ Multimicronutriente ]
into #num_Esq6m
from #denominador a
left join #num_Esq6m_Trat	b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo			-- Trama de	Esquema de 6 meses [Tratamiento]
left join #num_Esq6m_sup	b2	on a.num_doc=b2.num_doc and a.seguro=b2.seguro and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo			-- Trama de	Esquema de 6 meses [Suplementación] 
left join #num_Esq6m_Multi	b3	on a.num_doc=b3.num_doc and a.seguro=b3.seguro and a.fecha_nac=b3.fecha_nac and a.ubigeo=b3.ubigeo			-- Trama de	Esquema de 6 meses [Multimicronutrientes]


/* ===================================================================================================================================
									2.9 Dosaje de hemoglobina 

-------------------------------------------------------------------------------------------------
N°		Rango Anterior-Posterior		Rango Para Calificar	Numero Acumuluado	Cumple 
1											0-209				Todos				Todos
2			170-209							210-364				>=1					1						
====================================================================================================================================== */

/* 2.9.1 - Niñas y niños menores del denominador que han realizado dosaje de hemoglobina en sangre entre los 170-209 días identificado con código 85018 o 85018.01 (Tipo de diagnóstico “D”)
*/
if Object_id(N'tempdb..#num_Hb',N'U') is not null drop table #num_Hb;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_atencion fecha_Hb
				, num_Hb=1
into #num_Hb 
from #denominador  a inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('85018','85018.01') and b.tipo_dx='D'													-- Código de Dosaje de Hemoglobina
and (datediff(dd,a.fecha_nac,b.fecha_atencion) between 170 and 209)											-- Busqueda de dosaje entre los 170 a 209 dias 
and b.fecha_atencion<=a.fecha_final	


/* 2.9.2 - Niñas y niños menores del denominador que han recibido dosaje de Hemoglobina según edad */
if Object_id(N'tempdb..#num_dosaje_Hb',N'U') is not null drop table #num_dosaje_Hb;
select  *
, case	when edad_dias<=209	then 1
		when edad_dias>=210 and num_Hb=1 then 1 else 0 end num_dosaje_Hb
into #num_dosaje_Hb
from (
		select a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
		, min(b1.fecha_Hb)	fecha_Hb	,	max(isnull(b1.num_Hb,0))	num_Hb
		from #denominador	a
		left join #num_Hb  b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Trama de dosaje de Hemoglobina.
		group by a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
) as t 


/* ===================================================================================================================================
									2.10 EMISION DE DNI

-------------------------------------------------------------------------------------------------
N°		Rango Anterior-Posterior		Rango Para Calificar	Numero Acumuluado	Cumple (N° Frascos)
1											0-60				Todos				Todos
2			0-60							61-364				>=1					1
====================================================================================================================================== */

/* 2.10.1 - Niñas y niños menores del denominador que Cuentan con DNI emitido hasta los 60 dias de nacido, tener en cuenta el siguiente criterio
// Tiempo de emisión del DNI: el tiempo transcurrido entre la fecha de nacimiento del niño y la fecha de emisión de su DNI es igual o menor a 60 días de edad.
*/
/*&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&-----------ACTIVACION DE EMISION DE DNI-------------------------&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
--%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%*/
if Object_id(N'tempdb..#emision_dni',N'U') is not null drop table #emision_dni;
select distinct a.num_doc
				, a.fecha_nac
				, a.seguro
				, a.ubigeo
				, a.fecha_final
				, b.fecha_emision		Fecha_DNIemision
				, num_DniEmision_60d=1
				, iif((datediff(dd,a.fecha_nac,b.fecha_emision) between 0 and 30),1,0) num_DniEmision_30d
into #emision_dni
from #denominador  a inner join #emision b on a.num_doc=b.num_doc collate SQL_Latin1_General_CP1_CI_AS
where b.fecha_emision<=a.fecha_final																			-- La fecha de emision debe ser menor o igual al ultimo dia de evaluación.
and (datediff(dd,a.fecha_nac,b.fecha_emision) between 0 and 60)													-- La fecha de emisión de su DNI es igual o menor a 60 días de edad.


/*2.10.2 - Niñas y niños menores del denominador que Cuentan con DNI emitido hasta los 60 dias de nacido*/
if Object_id(N'tempdb..#num_DNIemision',N'U') is not null drop table #num_DNIemision;
select  *
, case	when edad_dias<=60 then 1
		when edad_dias>=61 and num_DNIemision_60d=1 then 1 else 0 end num_DNIemision
into #num_DNIemision
from (
		select a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
		, min(b1.Fecha_DNIemision)				fecha_DNIemision	
		, max(isnull(b1.num_DniEmision_60d,0))	num_DNIemision_60d
		, max(isnull(b1.num_DniEmision_30d,0))	num_DNIemision_30d
		from #denominador	a
		left join #emision_dni  b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Trama de dosaje de Hemoglobina.
		group by a.num_doc, a.fecha_nac, a.seguro, a.ubigeo, a.edad_dias
) as t 
/* */

--%%%%%%%%%%%%%%%%
-- 3.INDICADOR
--%%%%%%%%%%%%%%%%

-- 3.1 Reporte de union del denominador y numerador. 
insert into #tabla_reporte
select	
@año año
, @mes_inicio mes
, a.tipo_doc
, a.num_doc
, a.fecha_nac
, a.sexo
, a.ubigeo
, a.seguro
, a.edad_dias
, a.edad_mes
, a.flag_cnv
, a.Peso_cnv
, a.flag_BPN
, a.Semana_gest_cnv
, a.flag_prematuro
, a.flag_BPN_Prematuro
, a.flag_indicador
, denominador=1
, iif(b.num_cred_rn=1 and c.num_cred_mensual=1 and d.num_vac_antineumococica=1 and e.num_vac_antipolio=1 and g.num_vac_pentavalente=1 and h.num_vac_rotavirus=1 and i.num_Esq4m=1 and j.num_esq6m=1 and k.num_dosaje_Hb=1 and l.num_DNIemision=1,1,0) numerador
--, iif(b.num_cred_rn=1 and c.num_cred_mensual=1 and d.num_vac_antineumococica=1 and e.num_vac_antipolio=1 and g.num_vac_pentavalente=1 and h.num_vac_rotavirus=1 and i.num_Esq4m=1 and j.num_esq6m=1 and k.num_dosaje_Hb=1,1,0) numerador
, iif(b.num_cred_rn=1 and c.num_cred_mensual=1,1,0) num_cred
, b.num_cred_rn
, b.fecha_cred_rn1		, b.num_cred_rn1
, b.fecha_cred_rn2		, b.num_cred_rn2
, b.fecha_cred_rn3		, b.num_cred_rn3
, b.fecha_cred_rn4		, b.num_cred_rn4
, c.num_cred_mensual
, c.fecha_cred_mes1		, c.num_cred_mes1
, c.fecha_cred_mes2		, c.num_cred_mes2
, c.fecha_cred_mes3		, c.num_cred_mes3
, c.fecha_cred_mes4		, c.num_cred_mes4
, c.fecha_cred_mes5		, c.num_cred_mes5
, c.fecha_cred_mes6		, c.num_cred_mes6
, c.fecha_cred_mes7		, c.num_cred_mes7
, c.fecha_cred_mes8		, c.num_cred_mes8
, c.fecha_cred_mes9		, c.num_cred_mes9
, c.fecha_cred_mes10	, c.num_cred_mes10
, c.fecha_cred_mes11	, c.num_cred_mes11
, iif(d.num_vac_antineumococica=1 and e.num_vac_antipolio=1 and g.num_vac_pentavalente=1 and h.num_vac_rotavirus=1,1,0) num_vac
, d.num_vac_antineumococica
, d.fecha_vac_antineumococica1	, d.num_vac_antineumococica1
, d.fecha_vac_antineumococica2	, d.num_vac_antineumococica2
, e.num_vac_antipolio
, e.fecha_vac_antipolio1		, e.num_vac_antipolio1
, e.fecha_vac_antipolio2		, e.num_vac_antipolio2
, e.fecha_vac_antipolio3		, e.num_vac_antipolio3
, g.num_vac_pentavalente
, g.fecha_vac_pentavalente1		, g.num_vac_pentavalente1
, g.fecha_vac_pentavalente2		, g.num_vac_pentavalente2
, g.fecha_vac_pentavalente3		, g.num_vac_pentavalente3
, h.num_vac_rotavirus
, h.fecha_vac_rotavirus1		, h.num_vac_rotavirus1
, h.fecha_vac_rotavirus2		, h.num_vac_rotavirus2
, iif(i.num_Esq4m=1 and j.num_esq6m=1,1,0) num_Esq
, i.num_Esq4m
, i.fecha_Esq4m_Sup_E1			, i.num_Esq4m_Sup_E1
, j.num_esq6m
, j.num_Esq6m_sup
, j.fecha_Esq6m_Sup_E1			, j.num_Esq6m_Sup_E1
, j.fecha_Esq6m_Sup_E2			, j.num_Esq6m_Sup_E2
, j.num_Esq6m_Trat
, j.fecha_Esq6m_Trat_E1			, j.num_Esq6m_Trat_E1
, j.fecha_Esq6m_Trat_E2			, j.num_Esq6m_Trat_E2
, j.fecha_Esq6m_Trat_E3			, j.num_Esq6m_Trat_E3
, j.num_Esq6m_multi
, j.fecha_Esq6m_Multi_E1		, j.num_Esq6m_Multi_E1
, j.fecha_Esq6m_Multi_E2		, j.num_Esq6m_Multi_E2
, j.fecha_Esq6m_Multi_E3		, j.num_Esq6m_Multi_E3
, j.fecha_Esq6m_Multi_E4		, j.num_Esq6m_Multi_E4
, j.fecha_Esq6m_Multi_E5		, j.num_Esq6m_Multi_E5
, j.fecha_Esq6m_Multi_E6		, j.num_Esq6m_Multi_E6
, k.num_dosaje_Hb
, k.fecha_Hb					, k.num_Hb
, l.num_DNIemision
, l.fecha_DNIemision			, l.num_DNIemision_30d	,	l.num_DNIemision_60d
from #denominador a
left join #num_CRED_RN				b	on a.num_doc=b.num_doc and a.seguro=b.seguro and a.fecha_nac=b.fecha_nac and a.ubigeo=b.ubigeo
left join #num_CRED_mes				c	on a.num_doc=c.num_doc and a.seguro=c.seguro and a.fecha_nac=c.fecha_nac and a.ubigeo=c.ubigeo
left join #num_vac_antineumococica	d	on a.num_doc=d.num_doc and a.seguro=d.seguro and a.fecha_nac=d.fecha_nac and a.ubigeo=d.ubigeo
left join #num_vac_antipolio		e	on a.num_doc=e.num_doc and a.seguro=e.seguro and a.fecha_nac=e.fecha_nac and a.ubigeo=e.ubigeo
left join #num_vac_pentavalente		g	on a.num_doc=g.num_doc and a.seguro=g.seguro and a.fecha_nac=g.fecha_nac and a.ubigeo=g.ubigeo
left join #num_vac_rotavirus		h	on a.num_doc=h.num_doc and a.seguro=h.seguro and a.fecha_nac=h.fecha_nac and a.ubigeo=h.ubigeo
left join #num_Esq4m				i	on a.num_doc=i.num_doc and a.seguro=i.seguro and a.fecha_nac=i.fecha_nac and a.ubigeo=i.ubigeo		
left join #num_Esq6m				j	on a.num_doc=j.num_doc and a.seguro=j.seguro and a.fecha_nac=j.fecha_nac and a.ubigeo=j.ubigeo
left join #num_dosaje_Hb			k	on a.num_doc=k.num_doc and a.seguro=k.seguro and a.fecha_nac=k.fecha_nac and a.ubigeo=k.ubigeo
left join #num_DNIemision			l	on a.num_doc=l.num_doc and a.seguro=l.seguro and a.fecha_nac=l.fecha_nac and a.ubigeo=l.ubigeo


print(@mes_inicio)
print(@año)

set @mes_inicio = @mes_inicio + 1

-- Espera por un segundo (opcional)
-- WAITFOR DELAY '00:00:01';

end

-- SELECT * from #tabla_reporte
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

-- Tabla: REPORTE NOMINAL
if object_id(N'FED25.TRAMAHIS_FED2024_25_MC02_Nominal_2025', N'U') is not null drop table FED25.TRAMAHIS_FED2024_25_MC02_Nominal_2025;
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
into FED25.TRAMAHIS_FED2024_25_MC02_Nominal_2025
from #tabla_reporte a
inner join MaeUbigeo_20240808 b on convert(int,a.ubigeo)=convert(int,b.ubigeo)

-- Tabla: Reporte Consolidado
select periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, flag_indicador, flag_BPN, flag_prematuro, flag_BPN_Prematuro, seguro, sexo
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
, sum(denominador)					denominador
, sum(numerador)					numerador
, sum(num_cred)						num_cred
, sum(num_cred_mensual)				num_cred_mensual
, sum(num_cred_rn)					num_cred_rn
, sum(num_vac)						num_vac
, sum(num_vac_antineumococica)		num_vac_antineumococica
, sum(num_vac_antipolio)			num_vac_antipolio
, sum(num_vac_rotavirus)			num_vac_rotavirus
, sum(num_vac_pentavalente)			num_vac_pentavalente
, sum(num_esq)						num_esq
, sum(num_esq4M)					num_esq4M
, sum(num_esq6M)					num_esq6M
, sum(num_esq6M_sup)				num_esq6M_sup
, sum(num_esq6M_trat)				num_esq6M_trat
, sum(num_esq6M_multi)				num_esq6M_multi
, sum(num_dosaje_Hb)				num_dosaje_Hb
, sum(num_DNIemision)				num_DNIemision
from FED25.TRAMAHIS_FED2024_25_MC02_Nominal_2025
group by periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, flag_indicador, flag_BPN, flag_prematuro, flag_BPN_Prematuro, seguro, sexo
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
--------------------------------------------- =D

