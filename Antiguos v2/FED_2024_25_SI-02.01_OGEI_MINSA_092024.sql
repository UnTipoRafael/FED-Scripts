/*=======================================================================================================================================
Compromiso Gestion: SI-02 | Niñas y niños de seis (06) meses de edad que reciben tratamiento con hierro o suplementación 
							preventiva con hierro hasta los 12 meses de edad.

Indicador	:	SI-02.01  |	[Condición Previa]	- Diferenciado por ambito de aplicación.
				
				* Region:	Porcentaje de niñas y niños prematuros y/o bajo peso al nacer, y niñas y niños de cuatro (04) meses de edad del 
							departamento, sin diagnóstico de anemia; que reciben dosajes de hemoglobina y culminan la suplementación preventiva 
							con hierro a los 6 meses de edad.

				* Lima Metro:	Porcentaje de niñas y niños de cuatro (04) meses de edad de Lima Metropolitana
								, que reciben 01 frasco de sulfato ferroso y 01 dosaje de hemoglobina a los 6 meses de edad.

Elaboración	:	MINISTERIO DESARROLLO E INCLUSION SOCIAL (MIDIS)
				DIRECCIÓN DE SEGUIMIENTO Y EVALUACIÓN (DGSE)
				DIRECCIÓN DE SEGUIMIENTO (DS)
				/ JHONATAN LAVI
Revisión	:	MINISTERIO DE SALUD (MINSA)
				OFICINA GENERAL DE TECNOLOGIAS DE LA INFORMACION (OGTI)
				OFICINA DE GESTION DE LA INFORMACION (OGEI)
				Wilson URVIOLA ZAPATA - ANALISTA ESTADISTICO
Fecha		:	19/08/2024
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
					'99199.17',								--	códigos de Entrega de Hierro.
					'85018','85018.01',						--	códigos de Dosaje de Hb.
					'D500','D508','D509','D649','D539'		--	Códigos de Diagnostico de anemia.
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
					'99199.17',								--	códigos de Entrega de Hierro.
					'85018','85018.01',						--	códigos de Dosaje de Hb.
					'D500','D508','D509','D649','D539'		--	Códigos de Diagnostico de anemia.
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


/* ************************************************************************
					SINTAXIS

En esta parte se procede a generar una tabla temporal donde 
se van a insertar los valores del indicador.
El proceso de la sintaxis es un loop que va corriendo el indicador
mes a mes , para eso se debe colocar en @mes_eval el mes donde inicia
la busqueda de información del indicador y en @mes_final el mes donde
finaliza la busqueda de información (Periodo de evaluación).
*************************************************************************** */
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
flag_cnv				int,
peso_cnv				int,
Semana_gest_cnv			int,
fecha_dx				date,
den_dx					int,
flag_indicador			int,
denominador				int,
numerador				int,
den_SinBPN				int,
num_SinBPN				int,
fecha_sinBPN_hb_6m		date,
num_sinBPN_hb_6m		int,
fecha_SinBPN_sup_4m		date,
num_SinBPN_sup_4m		int,
den_BPN					int,
num_BPN					int,
fecha_bpn_Hb_30d		date,
num_bpn_Hb_30d			int,
fecha_bpn_sup_30d		date,
num_bpn_sup_30d			int,
fecha_bpn_Hb_3m			date,
num_bpn_Hb_3m			int,
fecha_bpn_sup_4m		date,
num_bpn_sup_4m			int,
fecha_bpn_Hb_6m			date,
num_bpn_Hb_6m			int,
) 
	
declare @mes_inicio int, 
		@mes_eval int, 
		@año int 

set @año=2025
set @mes_inicio= 1 
set @mes_eval= 1 --< Modificar segun mes de evaluación.

while @mes_inicio <= @mes_eval
begin

--%%%%%%%%%%%%%%%%
-- 1.DENOMINADOR
--%%%%%%%%%%%%%%%%

/* 1.1 - Punto Comun del denominador de Lima Metropolitana y Region(Diresa).
	Niñas y niños de seis (06) meses de edad (209 días), en el mes de medición, registrados en el padrón nominal (con DNI o CNV en línea). */ 
if Object_id(N'tempdb..#padron_den',N'U') is not null drop table #padron_den
select	tipo_doc,
		num_doc,
		fecha_nac,
		sexo,
		ubigeo,
		seguro,
		dateadd(dd,209,fecha_nac) fecha_corte
into #padron_den
from #padron_nominal
where year(dateadd(dd,209,fecha_nac))=@año
and month(dateadd(dd,209,fecha_nac))=@mes_inicio					-- cumplen 209 días en el mes de evaluación.


/* 1.2 - Punto Comun del denominador de Lima Metropolitana y Region(Diresa).
	Niños del denominador que cuentan con la siguiente condición en el CNV.
	* Para evaluar a las niñas y niños con prematuridad y/o bajo peso al nacer, se considera a las niñas y niños de 6 meses de edad con prematuridad (34-36 semanas de gestación) y/o bajo peso al nacer (1500-2499gr).
	* Para evaluar a las niñas y niños de 4 meses de edad, sin prematuridad y/o bajo peso al nacer, se excluye a las niñas y niños de 6 meses de edad prematuros (menor de 37 semanas de gestación) y sin bajo peso al nacer (menor a 2500gr). 
*/ 
if Object_id(N'tempdb..#cnv_bpn_premat',N'U') is not null drop table #cnv_bpn_premat;
select num_doc
, cnv=1
, max(peso)	peso
, max(sem_gestacion) sem_gestacion
, max(iif((peso between 1500 and 2499) or 
			(sem_gestacion between 34 and 36),1,0)) Eval_BPN		-- Para evaluar a las niñas y niños con prematuridad y/o bajo peso al nacer, se considera a las niñas y niños de 6 meses de edad con prematuridad (34-36 semanas de gestación) y/o bajo peso al nacer (1500-2499gr)
, max(iif( (peso<2500) or 
			(Sem_Gestacion<37),1,0))				Eval_SinBPN		-- Para evaluar a las niñas y niños de 4 meses de edad, sin prematuridad y/o bajo peso al nacer, se excluye a las niñas y niños de 6 meses de edad prematuros (menor de 37 semanas de gestación) y sin bajo peso al nacer (menor a 2500gr)
into #cnv_bpn_premat
from #cnv
group by num_doc


/* 1.3 - Punto DIFERENCIADOR del denominador de Lima Metropolitana y Region(Diresa).
	Niños que Se excluye del denominador a las niñas y niños identificados con diagnóstico de anemia entre los 0 a 170 dias de edad, registrados en el HIS con los 
	códigos CIEX: D500, D508, D509, D649, D539 (Tipo de diagnóstico definitivo “D”). 
*/ 
if Object_id(N'tempdb..#padron_Dx',N'U') is not null drop table #padron_Dx;
select	a.tipo_doc,
		a.num_doc,
		a.fecha_nac,
		a.sexo,
		a.ubigeo,
		a.seguro,
		min(b.fecha_atencion) fecha_dx,
		den_dx=1
into #padron_Dx
from #padron_den a
inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('D500','D508','D509','D649','D539') and b.tipo_dx='D'			-- Diagnostico de anemia. 
and ( b.fecha_atencion between a.fecha_nac and dateadd(dd,170,a.fecha_nac) )		-- Identificados entre los 0 a 170 dias de edad.
group by a.tipo_doc, a.num_doc,	a.fecha_nac,	
		a.sexo,	a.ubigeo, a.seguro

	
/* 1.4 - Unión de Información para armar el denominador del indicador. */
if Object_id(N'tempdb..#denominador',N'U') is not null drop table #denominador;
select 
  a.tipo_doc
, a.num_doc
, a.fecha_nac
, a.sexo
, a.ubigeo
, a.seguro
, a.fecha_corte
, isnull(b.cnv,0)				Flag_cnv
, isnull(b.peso,0)				Peso_cnv
, isnull(b.sem_gestacion,0)		Semana_gest_cnv	
, c.fecha_dx
, isnull(den_dx,0)				den_Dx
, iif(Eval_BPN=1,1,0)			den_BPN									-- Niñas y niños con prematuridad y/o bajo peso al nacer.
, iif(Eval_SinBPN=1,0,1)		den_SinBPN								-- Niñas y niños de 4 meses de edad, sin prematuridad y/o bajo peso al nacer
, iif(den_dx=1,0,1)				flag_diferenciador						-- Diferenciar el denominador de Lima Metropolitana y Region(Diresa) [Niños con anemia y Niños sin anemia entre los 0 a 170 dias de edad]
into #denominador
from #padron_den a
left join #cnv_bpn_premat	b	on a.num_doc=b.num_doc 
left join #padron_Dx		c	on a.num_doc=c.num_doc and a.seguro=c.seguro and a.fecha_nac=c.fecha_nac and a.ubigeo=c.ubigeo and a.sexo=c.sexo


--%%%%%%%%%%%%%%%%
-- 2.NUMERADOR
--%%%%%%%%%%%%%%%%

--============================================================================================================================================
	--			2.1	Niños Prematuros (34-36 semanas de gestación) y/o bajo peso al nacer (1500-2499gr)
--============================================================================================================================================

/* 2.1.1 -	Niños del denominador con BPN y/o Prematuro que Reciben un (01) dosaje de hemoglobina a los 30 días de nacido (se busca el dato entre 30-59 días)
			, registrado con los códigos: 85018 o 85018.01 (tipo de diagnóstico definitivo "D"). */
if Object_id(N'tempdb..#num_bpn_hb_30d',N'U') is not null drop table #num_bpn_hb_30d;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_bpn_Hb_30d,
		num_bpn_Hb_30d=1
into #num_bpn_Hb_30d
from #denominador	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where a.den_bpn=1																				-- Niños Prematuros y/o bajo peso al nacer
and b.cod_item in ('85018','85018.01')	and b.tipo_dx='D'										-- Dosaje de Hb.
and (b.fecha_atencion between dateadd(dd,30,a.fecha_nac) and dateadd(dd,59,a.fecha_nac))		-- Entrega a los 30 días de nacido (se busca el dato entre 30-59 días)
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


/* 2.1.2 -	Niños del denominador con BPN y/o Prematuro que Reciben un (01) frasco de hierro a los 30 días de nacido (se busca el dato entre 30-59 días), registrado con el código 99199.17.  */
if Object_id(N'tempdb..#num_bpn_sup_30d',N'U') is not null drop table #num_bpn_sup_30d;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		a.den_BPN,
		max(b.fecha_atencion) fecha_bpn_sup_30d,
		num_bpn_sup_30d=1
into #num_bpn_sup_30d
from #denominador	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where a.den_bpn=1																				-- Niños Prematuros y/o bajo peso al nacer
and b.cod_item='99199.17'																		-- Entrega de Hierro.
and (b.fecha_atencion between dateadd(dd,30,a.fecha_nac) and dateadd(dd,59,a.fecha_nac))		-- Entrega a los 30 días de nacido (se busca el dato entre 30-59 días)
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo, a.den_BPN


/* 2.1.3 -	Reciben un (01) segundo frasco de hierro a los 04 meses de edad (se busca el dato entre 110-130 días), registrado con el código 99199.17.  */
if Object_id(N'tempdb..#num_bpn_sup_4m',N'U') is not null drop table #num_bpn_sup_4m;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_bpn_sup_4m,
		num_bpn_sup_4m=1
into #num_bpn_sup_4m
from #denominador	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where a.den_bpn=1																				-- Niños Prematuros y/o bajo peso al nacer
and b.cod_item='99199.17'																		-- Entrega de Hierro.
and (b.fecha_atencion between dateadd(dd,110,a.fecha_nac) and dateadd(dd,130,a.fecha_nac))		-- Entrega a los 04 meses de edad (se busca el dato entre 110-130 días).
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


/* 2.1.4 -	Niños del denominador con BPN y/o Prematuro que Reciben un (01) dosaje de hemoglobina de control a los tres (03) meses de iniciada la suplementación 
			(se busca el dato entre 90-119 días), registrado con códigos: 85018 o 85018.01 (tipo de diagnóstico definitivo "D"). */
if Object_id(N'tempdb..#num_bpn_hb_3m',N'U') is not null drop table #num_bpn_hb_3m;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_bpn_Hb_3m,
		num_bpn_Hb_3m=1
into #num_bpn_Hb_3m
from #num_bpn_sup_30d	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where a.den_bpn=1																							-- Niños Prematuros y/o bajo peso al nacer
and b.cod_item in ('85018','85018.01')	and b.tipo_dx='D'													-- Dosaje de Hb.
and (b.fecha_atencion between dateadd(dd,90,a.fecha_bpn_sup_30d) and dateadd(dd,119,a.fecha_bpn_sup_30d))	-- Entrega a los tres (03) meses de iniciada la suplementación (se busca el dato entre 90-119 días)
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


/* 2.1.5 -	Niños del denominador con BPN y/o Prematuro que Reciben Reciben un (01) dosaje de hemoglobina a los seis (06) meses de edad 
			(se busca el dato entre 170-209 dias), registrado con códigos: 85018 o 85018.01 (tipo de diagnóstico definitivo "D") */
if Object_id(N'tempdb..#num_bpn_hb_6m',N'U') is not null drop table #num_bpn_hb_6m;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_bpn_Hb_6m,
		num_bpn_Hb_6m=1
into #num_bpn_Hb_6m
from #denominador	a	
inner join #his_minsa	b on a.num_doc=b.num_doc
where a.den_bpn=1																			-- Niños Prematuros y/o bajo peso al nacer
and b.cod_item in ('85018','85018.01')	and b.tipo_dx='D'									-- Dosaje de Hb.
and (b.fecha_atencion between dateadd(dd,170,a.fecha_nac) and dateadd(dd,209,a.fecha_nac))	-- Entrega a los seis (06) meses de edad (se busca el dato entre 170-209 dias)
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


/* 2.1.6 - Niños y Niñas del denominador Prematuros y/o Bajo peso al nacer, que cuentan con la entrega de servicios */
if Object_id(N'tempdb..#num_bpn',N'U') is not null drop table #num_bpn;
select distinct	a.tipo_doc, 
				a.num_doc, 
				a.fecha_nac, 
				a.ubigeo, 
				a.seguro, 
				b1.fecha_bpn_Hb_30d		,	isnull(b1.num_bpn_Hb_30d,0)		num_bpn_Hb_30d,
				b2.fecha_bpn_sup_30d	,	isnull(b2.num_bpn_sup_30d,0)	num_bpn_sup_30d,
				b3.fecha_bpn_Hb_3m		,	isnull(b3.num_bpn_Hb_3m,0)		num_bpn_Hb_3m,
				b4.fecha_bpn_sup_4m		,	isnull(b4.num_bpn_sup_4m,0)		num_bpn_sup_4m,
				b5.fecha_bpn_Hb_6m		,	isnull(b5.num_bpn_Hb_6m,0)		num_bpn_Hb_6m,
				iif(b1.num_bpn_Hb_30d=1 and b2.num_bpn_sup_30d=1 and b3.num_bpn_Hb_3m=1
					and b4.num_bpn_sup_4m=1 and b5.num_bpn_Hb_6m=1,1,0)		num_bpn
into #num_bpn
from #denominador a
left join #num_bpn_Hb_30d	b1	on a.seguro=b1.seguro and a.num_doc=b1.num_doc and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Entrega de Hb a los 30 dias.
left join #num_bpn_sup_30d	b2	on a.seguro=b2.seguro and a.num_doc=b2.num_doc and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo				-- Entrega de Hierro a los 30 dias.
left join #num_bpn_Hb_3m	b3	on a.seguro=b3.seguro and a.num_doc=b3.num_doc and a.fecha_nac=b3.fecha_nac and a.ubigeo=b3.ubigeo				-- Entrega de Hb a los 3 meses de iniciado la suplementacion.
left join #num_bpn_sup_4m	b4	on a.seguro=b4.seguro and a.num_doc=b4.num_doc and a.fecha_nac=b4.fecha_nac and a.ubigeo=b4.ubigeo				-- Entrega de Hierro a los 4 meses.
left join #num_bpn_Hb_6m	b5	on a.seguro=b5.seguro and a.num_doc=b5.num_doc and a.fecha_nac=b5.fecha_nac and a.ubigeo=b5.ubigeo				-- TEntrega de Hb a los 6 meses.


--============================================================================================================================================
	--			2.2	Niños sin prematuridad: menor de 37 semanas de gestación; y sin bajo peso al nacer: menor a 2500g
--============================================================================================================================================

/* 2.2.1 -	Niños del denominador sin BPN y/o Prematuro que Reciben un (01) frasco de hierro a los cuatro (04) meses de edad (se busca el dato entre 110-130 días), registrado con código 99199.17 . */
if Object_id(N'tempdb..#num_SinBPN_sup_4m',N'U') is not null drop table #num_SinBPN_sup_4m;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_SinBPN_sup_4m,
		num_SinBPN_sup_4m=1
into #num_SinBPN_sup_4m
from #denominador	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where a.den_SinBPN=1																			-- Niños sin bajo peso al nacer y/o Prematuros .
and b.cod_item='99199.17'																		-- Entrega de Hierro.
and (b.fecha_atencion between dateadd(dd,110,a.fecha_nac) and dateadd(dd,130,a.fecha_nac))		-- Entrega a los cuatro (04) meses de edad (se busca el dato entre 110-130 días).
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


/* 2.2.2 -	Niños del denominador sin BPN y/o Prematuro que Reciben un (01) dosaje de hemoglobina a los seis (06) meses de edad (se busca el dato entre 170-209 dias)
			, registrado con códigos: 85018 o 85018.01 (tipo de diagnóstico definitivo "D"). */
if Object_id(N'tempdb..#num_SinBPN_hb_6m',N'U') is not null drop table #num_SinBPN_hb_6m;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_SinBPN_hb_6m,
		num_SinBPN_hb_6m=1
into #num_SinBPN_hb_6m
from #denominador	a	
inner join #his_minsa	b on a.num_doc=b.num_doc
where a.den_SinBPN=1																		-- Niños sin bajo peso al nacer y/o Prematuros .
and b.cod_item in ('85018','85018.01')	and b.tipo_dx='D'									-- Dosaje de Hb.
and (b.fecha_atencion between dateadd(dd,170,a.fecha_nac) and dateadd(dd,209,a.fecha_nac))	-- Entrega a los seis (06) meses de edad (se busca el dato entre 170-209 dias).
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


/* 2.2.3 - Niños y Niñas del denominador Sin Bajo peso y/o Prematuros al nacer, que cuentan con la entrega de servicios */
if Object_id(N'tempdb..#num_SinBPN',N'U') is not null drop table #num_SinBPN;
select distinct	a.tipo_doc, 
				a.num_doc, 
				a.fecha_nac, 
				a.ubigeo, 
				a.seguro, 
				b1.fecha_SinBPN_hb_6m	,	isnull(b1.num_SinBPN_hb_6m,0)	num_SinBPN_hb_6m,
				b2.fecha_SinBPN_sup_4m	,	isnull(b2.num_SinBPN_sup_4m,0)	num_SinBPN_sup_4m,
				iif(b1.num_SinBPN_hb_6m=1 and b2.num_SinBPN_sup_4m=1,1,0)	num_SinBPN
into #num_SinBPN
from #denominador a
left join #num_SinBPN_hb_6m		b1	on a.seguro=b1.seguro and a.num_doc=b1.num_doc and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Entrega de Hb a los 6 meses.
left join #num_SinBPN_sup_4m	b2	on a.seguro=b2.seguro and a.num_doc=b2.num_doc and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo				-- Entrega de Hierro a los 6 meses.


--%%%%%%%%%%%%%%%%%%
-- 3. INDICADOR
--%%%%%%%%%%%%%%%%%%

-- 3.1 Reporte de union del denominador y numerador. 
insert into #tabla_reporte
select @año año
, @mes_inicio mes
, a.tipo_doc
, a.num_doc
, a.fecha_nac
, a.sexo
, a.ubigeo
, a.seguro
, a.flag_cnv
, a.Peso_cnv
, a.Semana_gest_cnv
, a.fecha_dx
, a.den_Dx
, a.flag_diferenciador
, iif(a.den_sinBPN=1 or a.den_BPN=1,1,0)	denominador
, iif(b1.num_bpn=1 or b2.num_SinBPN=1,1,0)	numerador
, a.den_SinBPN
, b2.num_SinBPN
, b2.fecha_SinBPN_hb_6m	,	b2.num_SinBPN_hb_6m
, b2.fecha_SinBPN_sup_4m,	b2.num_SinBPN_sup_4m
, a.den_BPN
, b1.num_bpn
, b1.fecha_bpn_Hb_30d	,	b1.num_bpn_Hb_30d
, b1.fecha_bpn_sup_30d	,	b1.num_bpn_sup_30d
, b1.fecha_bpn_Hb_3m	,	b1.num_bpn_Hb_3m
, b1.fecha_bpn_sup_4m	,	b1.num_bpn_sup_4m
, b1.fecha_bpn_Hb_6m	,	b1.num_bpn_Hb_6m
from #denominador a
left join #num_bpn				b1	on a.seguro=b1.seguro and a.num_doc=b1.num_doc and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Dosaje y Entrega de Hierro a Prematuros Y/O BPN.
left join #num_SinBPN			b2	on a.seguro=b2.seguro and a.num_doc=b2.num_doc and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo				-- Dosaje y Entrega de Hierro a no Prematuros Y/O BPN.


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

-- Tabla: REPORTE NOMINAL
if object_id(N'FED25.TRAMAHIS_FED2024_25_SI_0201_Nominal_2025', N'U') is not null drop table FED25.TRAMAHIS_FED2024_25_SI_0201_Nominal_2025;
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
into FED25.TRAMAHIS_FED2024_25_SI_0201_Nominal_2025
from #tabla_reporte a
inner join MaeUbigeo_20240808 b on convert(int,a.ubigeo)=convert(int,b.ubigeo)

-- Tabla: Reporte Consolidado
select periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, flag_indicador, seguro, sexo
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
, sum(denominador)		denominador
, sum(numerador)		numerador
, sum(den_SinBPN)			den_SinBPN
, sum(num_SinBPN)			num_SinBPN
, sum(num_SinBPN_hb_6m)		num_SinBPN_hb_6m
, sum(num_SinBPN_sup_4m)	num_SinBPN_sup_4m
, sum(den_BPN)				den_BPN
, sum(num_bpn)				num_bpn
, sum(num_bpn_Hb_30d)		num_bpn_Hb_30d
, sum(num_bpn_sup_30d)		num_bpn_sup_30d
, sum(num_bpn_Hb_3m)		num_bpn_Hb_3m
, sum(num_bpn_sup_4m)		num_bpn_sup_4m
, sum(num_bpn_Hb_6m)		num_bpn_Hb_6m
-- from #TRAMAHIS_FED2024_25_SI_0201_Nominal
from FED25.TRAMAHIS_FED2024_25_SI_0201_Nominal_2025
group by periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, flag_indicador, seguro, sexo
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
------------------------------------------------------- =)
