/*=======================================================================================================================================
Compromiso Gestion: SI-02 | Niñas y niños de seis (06) meses de edad que reciben tratamiento con hierro o suplementación 
							preventiva con hierro hasta los 12 meses de edad.

Indicador	:	SI-02.03  |	Porcentaje de niñas y niños de seis (06) meses de edad, sin diagnóstico anemia, que reciben tres (03) 
							dosajes de hemoglobina y culminan la suplementación preventiva con hierro a los 12 meses de edad.

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
					'99199.17','99199.19',					--	códigos de Entrega de Hierro (Suplementación preventiva).
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
					'99199.17','99199.19',					--	códigos de Entrega de Hierro (Suplementación preventiva).
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
fecha_hb				date,
den_hb					int,
fecha_dx				date,
den_dx					int,
denominador				int,
numerador				int,
num_sup1				int,
num_hb_sup1_3m			int,
num_sup_cont			int,
num_sup_TA				int,
fecha_sup1_po			date,
num_sup1_po				int,
fecha_hb_sup1_po_3m		date,
num_hb_sup1_po_3m		int,
fecha_sup2_po			date,
num_sup2_po				int,
num_sup_po_TA			int,
fecha_sup1_multi		date,
num_sup1_multi			int,
fecha_hb_sup1_multi_3m	date,
num_hb_sup1_multi_3m	int,
fecha_sup2_multi		date,
num_sup2_multi			int,
fecha_sup3_multi		date,
num_sup3_multi			int,
fecha_sup4_multi		date,
num_sup4_multi			int,
fecha_sup5_multi		date,
num_sup5_multi			int,
fecha_sup6_multi		date,
num_sup6_multi			int,
num_sup_multi_TA		int,
fecha_ta				date,
num_ta					int,
fecha_hb_12m			date,
num_hb_12m				int
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

/* 1.1 -	Niñas y niños de 12 meses 29 días de edad (394 días), en el mes de medición, registrados en el padrón nominal con DNI 
			y CNV en línea, tipo de seguro MINSA */
if Object_id(N'tempdb..#padron_den',N'U') is not null drop table #padron_den
select	tipo_doc,
		num_doc,
		fecha_nac,
		sexo,
		ubigeo,
		seguro,
		dateadd(dd,394,fecha_nac) fecha_corte
into #padron_den
from #padron_nominal
where year(dateadd(dd,394,fecha_nac))=@año
and month(dateadd(dd,394,fecha_nac))=@mes_inicio					-- cumplen 12 meses 29 días de edad (394 días) en el mes de medición.


/* 1.2 -	Niñas y Niños del denominador que Reciben un (01) dosaje de hemoglobina a los seis (06) meses de edad (se busca el dato entre 170-209 dias)
		, registrado con los códigos: 85018 o 85018.01 (tipo de diagnóstico definitivo "D") */
if Object_id(N'tempdb..#padron_hb',N'U') is not null drop table #padron_hb;
select	a.tipo_doc,
		a.num_doc,
		a.fecha_nac,
		a.sexo,
		a.ubigeo,
		a.seguro,
		min(b.fecha_atencion) fecha_hb,
		den_hb=1
into #padron_hb
from #padron_den a
inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('85018','85018.01') and b.tipo_dx='D'											-- Dosaje de Hb.
and ( b.fecha_atencion between dateadd(dd,170,a.fecha_nac) and dateadd(dd,209,a.fecha_nac) )		-- Identificados a los seis (06) meses de edad (se busca el dato entre 170-209 dias)
group by a.tipo_doc, a.num_doc,	a.fecha_nac,	
		a.sexo,	a.ubigeo, a.seguro


/* 1.3 - Niñas y Niños del denominador sin diagnóstico de anemia registrado a los seis (06) meses de edad (se busca el dato entre 170-209 dias) con CIE-10: D500, D508, D509, D649 
o D539 (tipo de diagnóstico definitivo "D"), a partir del primer dosaje de hemoglobina */ 
if Object_id(N'tempdb..#padron_Dx',N'U') is not null drop table #padron_Dx;
select	a.tipo_doc,
		a.num_doc,
		a.fecha_nac,
		a.sexo,
		a.ubigeo,
		a.seguro,
		max(b.fecha_atencion) fecha_dx,
		den_dx=1
into #padron_Dx
from #padron_hb a
inner join #his_minsa b on a.num_doc=b.num_doc
where b.cod_item in ('D500','D508','D509','D649','D539') and b.tipo_dx='D'							-- Diagnostico de anemia. 
and ( b.fecha_atencion between dateadd(dd,170,a.fecha_nac) and dateadd(dd,209,a.fecha_nac) )		-- Identificados a los seis (06) meses de edad (se busca el dato entre 170-209 dias)
and b.fecha_atencion>=a.fecha_hb																	-- Identificados a partir del primer dosaje de hemoglobina
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
, b.fecha_hb
, isnull(b.den_hb,0)	den_hb
, c.fecha_dx
, isnull(c.den_dx,0)	den_dx
, iif(b.den_hb=1 and c.den_dx is null,1,0) denominador						-- Niño/as sin diagnóstico de anemia
into #denominador
from #padron_den a
left join #padron_hb		b	on a.num_doc=b.num_doc and a.seguro=b.seguro and a.fecha_nac=b.fecha_nac and a.ubigeo=b.ubigeo and a.sexo=b.sexo
left join #padron_Dx		c	on a.num_doc=c.num_doc and a.seguro=c.seguro and a.fecha_nac=c.fecha_nac and a.ubigeo=c.ubigeo and a.sexo=c.sexo


--%%%%%%%%%%%%%%%%
-- 2.NUMERADOR
--%%%%%%%%%%%%%%%%

/* 2.1 -	Niñas y niños del denominador, registrados que Reciben un (01) frasco de hierro, el mismo día del primer dosaje de hemoglobina a los seis (06) meses de edad 
			(se busca el dato entre 170- 209 días de edad), registrados con código 99199.17 o 99199.19.
			// La evaluación de las entregas de hierro es independientemente del LAB.
*/
-- 2.1.1 Niñas y niños del denominador que Reciben un (01) frasco de hierro.
if Object_id(N'tempdb..#num_sup1_po',N'U') is not null drop table #num_sup1_po;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_sup1_po,
		num_sup1_po=1
into #num_sup1_po
from #denominador	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where a.denominador=1														-- Niños del denominador.
and b.cod_item in ('99199.17')												-- Entrega de Hierro.
and b.fecha_atencion=a.fecha_hb												-- Entrega el mismo día del primer dosaje de hemoglobina.
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


-- 2.1.2 Niñas y niños del denominador que Reciben una (01) entrega de multimicronutrientes.  
if Object_id(N'tempdb..#num_sup1_multi',N'U') is not null drop table #num_sup1_multi;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_sup1_multi,
		num_sup1_multi=1
into #num_sup1_multi
from #denominador	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where a.denominador=1														-- Niños del denominador.
and b.cod_item in ('99199.19')												-- Entrega de multimicronutrientes.
and b.fecha_atencion=a.fecha_hb												-- Entrega el mismo día del primer dosaje de hemoglobina.
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


-- 2.1.3 Niñas y niños del denominador que Reciben una (01) entrega de Hierro o una (01) entrega de multimicronutrientes .
if Object_id(N'tempdb..#num_sup1',N'U') is not null drop table #num_sup1;
select a.*
, iif(b1.num_sup1_po=1 or b2.num_sup1_multi=1,1,0)			num_sup1
, b1.fecha_sup1_po			, isnull(b1.num_sup1_po,0)		num_sup1_po
, b2.fecha_sup1_multi		, isnull(b2.num_sup1_multi,0)	num_sup1_multi
into #num_sup1 
from #denominador a
left join #num_sup1_po			b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo 
left join #num_sup1_multi		b2	on a.num_doc=b2.num_doc and a.seguro=b2.seguro and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo 


/* 2.2 -	Niñas y niños del denominador, registrados que Reciben un (01) dosaje de hemoglobina de control al tercer mes de iniciada la suplementación (se busca el dato entre 90 - 119 días)
			, regitrado con códigos: 85018 o 85018.01 (tipo de diagnóstico definitivo "D").
*/
-- 2.2.1 Niñas y niños del denominador que Recibieron polimaltosado y dosaje de hemoglobina de control.
if Object_id(N'tempdb..#num_hb_sup1_po_3m',N'U') is not null drop table #num_hb_sup1_po_3m;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_hb_sup1_po_3m,
		num_hb_sup1_po_3m=1
into #num_hb_sup1_po_3m
from #num_sup1_po	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where b.cod_item in ('85018','85018.01') and b.tipo_dx='D'												-- Dosaje de Hb.
and (b.fecha_atencion between dateadd(dd,90,a.fecha_sup1_po) and dateadd(dd,119,a.fecha_sup1_po) )		-- Entrega al tercer mes de iniciada la suplementación (se busca el dato entre 90 - 119 días)
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


-- 2.2.2 Niñas y niños del denominador que Recibieron multimicronutrientes y dosaje de hemoglobina de control.
if Object_id(N'tempdb..#num_hb_sup1_multi_3m',N'U') is not null drop table #num_hb_sup1_multi_3m;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_hb_sup1_multi_3m,
		num_hb_sup1_multi_3m=1
into #num_hb_sup1_multi_3m
from #num_sup1_multi	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where b.cod_item in ('85018','85018.01') and b.tipo_dx='D'													-- Dosaje de Hb.
and (b.fecha_atencion between dateadd(dd,90,a.fecha_sup1_multi) and dateadd(dd,119,a.fecha_sup1_multi) )	-- Entrega al tercer mes de iniciada la suplementación (se busca el dato entre 90 - 119 días)
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


-- 2.2.3 Niñas y niños del denominador que Reciben un dosaje de hemoglobina de control al tercer mes de iniciada la suplementación.
if Object_id(N'tempdb..#num_hb_sup1_3m',N'U') is not null drop table #num_hb_sup1_3m;
select a.*
, iif(b1.num_hb_sup1_po_3m=1 or b2.num_hb_sup1_multi_3m=1,1,0)			num_hb_sup1_3m
, b1.fecha_hb_sup1_po_3m		, isnull(b1.num_hb_sup1_po_3m,0)		num_hb_sup1_po_3m
, b2.fecha_hb_sup1_multi_3m		, isnull(b2.num_hb_sup1_multi_3m,0)		num_hb_sup1_multi_3m
into #num_hb_sup1_3m
from #denominador a
left join #num_hb_sup1_po_3m		b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo 
left join #num_hb_sup1_multi_3m		b2	on a.num_doc=b2.num_doc and a.seguro=b2.seguro and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo 

	
/* 2.3 - Niñas y niños del denominador, que Reciben al menos dos (02) frascos de hierro* (incluye la entrega de hierro de inicio de suplementación preventiva), registrados con códigos 99199.17 o 
		99199.19 y que culmina con un (01) registro TA a los seis (06) meses de iniciada la suplementación preventiva (se busca el dato entre los 170 - 209 días), 
		registrados con código 99199.17 + lab:TA o 99199.19 + lab:TA.

	// - En los casos que existan de 02 o más entregas de hierro, se evalúa que cada entrega tenga un intervalo entre 25 a 70 días.
	// - La evaluación de las entregas de hierro es independientemente del LAB.
	// - En el caso de Multimicronutrientes (991499.19), se considera 6 sobres,se evalúa que cada entrega tenga un intervalo entre 25 a 30 días.
*/

-- 2.3.1 - Niñas y niños del denominador que Reciben un segundo (02) frasco de hierro.
if Object_id(N'tempdb..#num_sup2_po',N'U') is not null drop table #num_sup2_po;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_sup2_po,
		num_sup2_po=1
into #num_sup2_po
from #num_sup1_po	a
inner join #his_minsa	b on a.num_doc=b.num_doc
and b.cod_item in ('99199.17')																			-- Entrega de Hierro.
and (b.fecha_atencion between dateadd(dd,25,a.fecha_sup1_po) and dateadd(dd,70,a.fecha_sup1_po) )		-- Entrega tenga un intervalo entre 25 a 70 días.
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


-- 2.3.2 - Niñas y niños del denominador que Reciben una segunda (02) entrega de multimicronutrientes.  
if Object_id(N'tempdb..#num_sup2_multi',N'U') is not null drop table #num_sup2_multi;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_sup2_multi,
		num_sup2_multi=1
into #num_sup2_multi
from #num_sup1_multi a
inner join #his_minsa	b on a.num_doc=b.num_doc
and b.cod_item in ('99199.19')																				-- Entrega de multimicronutrientes.
and (b.fecha_atencion between dateadd(dd,25,a.fecha_sup1_multi) and dateadd(dd,30,a.fecha_sup1_multi) )		-- Entrega tenga un intervalo entre 25 a 30 días.
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo

-- 2.3.3 - Niñas y niños del denominador que Reciben una tercera (03) entrega de multimicronutrientes.  
if Object_id(N'tempdb..#num_sup3_multi',N'U') is not null drop table #num_sup3_multi;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_sup3_multi,
		num_sup3_multi=1
into #num_sup3_multi
from #num_sup2_multi a
inner join #his_minsa	b on a.num_doc=b.num_doc
and b.cod_item in ('99199.19')																				-- Entrega de multimicronutrientes.
and (b.fecha_atencion between dateadd(dd,25,a.fecha_sup2_multi) and dateadd(dd,30,a.fecha_sup2_multi) )		-- Entrega tenga un intervalo entre 25 a 30 días.
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo

-- 2.3.4 - Niñas y niños del denominador que Reciben una cuarta (04) entrega de multimicronutrientes.  
if Object_id(N'tempdb..#num_sup4_multi',N'U') is not null drop table #num_sup4_multi;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_sup4_multi,
		num_sup4_multi=1
into #num_sup4_multi
from #num_sup3_multi a
inner join #his_minsa	b on a.num_doc=b.num_doc
and b.cod_item in ('99199.19')																				-- Entrega de multimicronutrientes.
and (b.fecha_atencion between dateadd(dd,25,a.fecha_sup3_multi) and dateadd(dd,30,a.fecha_sup3_multi) )		-- Entrega tenga un intervalo entre 25 a 30 días.
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo

-- 2.3.5 - Niñas y niños del denominador que Reciben una quinta (05) entrega de multimicronutrientes.  
if Object_id(N'tempdb..#num_sup5_multi',N'U') is not null drop table #num_sup5_multi;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_sup5_multi,
		num_sup5_multi=1
into #num_sup5_multi
from #num_sup4_multi a
inner join #his_minsa	b on a.num_doc=b.num_doc
and b.cod_item in ('99199.19')																				-- Entrega de multimicronutrientes.
and (b.fecha_atencion between dateadd(dd,25,a.fecha_sup4_multi) and dateadd(dd,30,a.fecha_sup4_multi) )		-- Entrega tenga un intervalo entre 25 a 30 días.
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo

-- 2.3.6 - Niñas y niños del denominador que Reciben una sexta (06) entrega de multimicronutrientes.  
if Object_id(N'tempdb..#num_sup6_multi',N'U') is not null drop table #num_sup6_multi;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_sup6_multi,
		num_sup6_multi=1
into #num_sup6_multi
from #num_sup5_multi a
inner join #his_minsa	b on a.num_doc=b.num_doc
and b.cod_item in ('99199.19')																				-- Entrega de multimicronutrientes.
and (b.fecha_atencion between dateadd(dd,25,a.fecha_sup5_multi) and dateadd(dd,30,a.fecha_sup5_multi) )		-- Entrega tenga un intervalo entre 25 a 30 días.
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


/* 2.3.7 - Culmina Entrega de Hierro con un (01) registro TA, Registrado a los seis (06) meses de iniciada la suplementación preventiva (se busca el dato entre los 170 - 209 días).	
		[ Bajo el contexto de la ficha tecnica la suplementación preventiva es el mismo dia que el dosaje]
*/
if Object_id(N'tempdb..#num_TA',N'U') is not null drop table #num_TA;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		min(c.fecha_atencion) fecha_TA,
		num_TA=1
into #num_TA
from #num_sup1 a
inner join #his_minsa c on a.num_doc=c.num_doc
where a.num_sup1=1																					-- Cuentan con inicio de suplementación preventiva 
and c.cod_item in ('99199.17','99199.19') and c.valor_lab='TA'										-- Entrega de Hierro o multimicronutrientes que culmina con un (01) registro TA
and (c.fecha_atencion between dateadd(dd,170,a.fecha_hb) and dateadd(dd,209,a.fecha_hb) )			-- Registrado a los seis (06) meses de iniciado el tratamiento (se busca el dato entre 170-209 dias)
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


-- 2.3.8 Niñas y niños del denominador que Reciben dos (02) entregas de Hierro o seis (06) entrega de multimicronutrientes, culminando con un registro con TA.
if Object_id(N'tempdb..#num_sup_cont',N'U') is not null drop table #num_sup_cont;
select a.*, 
iif( (b1.num_sup1_po=1 and b2.num_sup2_po=1)
or (c1.num_sup1_multi=1 and c2.num_sup2_multi=1 and c3.num_sup3_multi=1 and
	c4.num_sup4_multi=1 and c5.num_sup5_multi=1 and	c6.num_sup6_multi=1),1,0)	num_sup_cont
, b2.fecha_sup2_po			, isnull(b2.num_sup2_po,0)		num_sup2_po
, c2.fecha_sup2_multi		, isnull(c2.num_sup2_multi,0)	num_sup2_multi
, c3.fecha_sup3_multi		, isnull(c3.num_sup3_multi,0)	num_sup3_multi
, c4.fecha_sup4_multi		, isnull(c4.num_sup4_multi,0)	num_sup4_multi
, c5.fecha_sup5_multi		, isnull(c5.num_sup5_multi,0)	num_sup5_multi
, c6.fecha_sup6_multi		, isnull(c6.num_sup6_multi,0)	num_sup6_multi
, d.fecha_TA				, isnull(d.num_TA,0)	num_TA
into #num_sup_cont
from #denominador a
left join #num_sup1_po		b1	on a.num_doc=b1.num_doc and a.seguro=b1.seguro and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo 
left join #num_sup2_po		b2	on a.num_doc=b2.num_doc and a.seguro=b2.seguro and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo 
left join #num_sup1_multi	c1	on a.num_doc=c1.num_doc and a.seguro=c1.seguro and a.fecha_nac=c1.fecha_nac and a.ubigeo=c1.ubigeo
left join #num_sup2_multi	c2	on a.num_doc=c2.num_doc and a.seguro=c2.seguro and a.fecha_nac=c2.fecha_nac and a.ubigeo=c2.ubigeo 
left join #num_sup3_multi	c3	on a.num_doc=c3.num_doc and a.seguro=c3.seguro and a.fecha_nac=c3.fecha_nac and a.ubigeo=c3.ubigeo 
left join #num_sup4_multi	c4	on a.num_doc=c4.num_doc and a.seguro=c4.seguro and a.fecha_nac=c4.fecha_nac and a.ubigeo=c4.ubigeo 
left join #num_sup5_multi	c5	on a.num_doc=c5.num_doc and a.seguro=c5.seguro and a.fecha_nac=c5.fecha_nac and a.ubigeo=c5.ubigeo
left join #num_sup6_multi	c6	on a.num_doc=c6.num_doc and a.seguro=c6.seguro and a.fecha_nac=c6.fecha_nac and a.ubigeo=c6.ubigeo
left join #num_TA			d	on a.num_doc=d.num_doc and a.seguro=d.seguro and a.fecha_nac=d.fecha_nac and a.ubigeo=d.ubigeo 


/* 2.4 -	Niñas y niños del denominador, registrados Reciben un (01) dosaje de hemoglobina a los 12 meses de edad (se busca el dato entre entre 365 días a 394 días)
			, registrados con códigos: 85018 o 85018.01 (tipo de diagnóstico definitivo "D")
*/
if Object_id(N'tempdb..#num_Hb_12m',N'U') is not null drop table #num_Hb_12m;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_Hb_12m,
		num_Hb_12m=1
into #num_Hb_12m
from #denominador	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where a.denominador=1																			-- Niños del denominador.
and b.cod_item in ('85018','85018.01') and b.tipo_dx='D'										-- Dosaje de Hb.
and (b.fecha_atencion between dateadd(dd,365,a.fecha_nac) and dateadd(dd,394,a.fecha_nac) )		-- Entrega a los 12 meses de edad (se busca el dato entre entre 365 días a 394 días)
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


--%%%%%%%%%%%%%%%%
--	3.INDICADOR
--%%%%%%%%%%%%%%%% 

--3.1 - Se procede a unir el denominador con los atributos del numerador para elaborar el reporte final.
insert into #tabla_reporte
select 
  @año			año
, @mes_inicio	mes
, a.tipo_doc
, a.num_doc
, a.fecha_nac
, a.sexo
, a.ubigeo
, a.seguro
, a.fecha_hb
, a.den_hb
, a.fecha_dx
, a.den_dx
, a.denominador
, iif( b.num_sup1=1 and c.num_hb_sup1_3m=1 and d.num_sup_cont=1 and d.num_TA=1 and e.num_Hb_12m=1,1,0) numerador
, b.num_sup1
, c.num_hb_sup1_3m
, d.num_sup_cont
, iif(d.num_sup_cont=1 and d.num_ta=1,1,0) num_sup_TA
-- Individual 
, b.fecha_sup1_po								,	b.num_sup1_po
, c.fecha_hb_sup1_po_3m							,	c.num_hb_sup1_po_3m
, d.fecha_sup2_po								,	d.num_sup2_po
, iif(d.num_sup2_po=1 and d.num_TA=1,1,0)		num_sup_po_TA
, b.fecha_sup1_multi							,	b.num_sup1_multi
, c.fecha_hb_sup1_multi_3m						,	c.num_hb_sup1_multi_3m
, d.fecha_sup2_multi							,	d.num_sup2_multi
, d.fecha_sup3_multi							,	d.num_sup3_multi
, d.fecha_sup4_multi							,	d.num_sup4_multi
, d.fecha_sup5_multi							,	d.num_sup5_multi
, d.fecha_sup6_multi							,	d.num_sup6_multi
, iif(d.num_sup6_multi=1 and d.num_TA=1,1,0)	num_sup_multi_TA
, d.fecha_TA									,	d.num_TA
, e.fecha_Hb_12m								,	isnull(e.num_Hb_12m,0) num_Hb_12m
from #denominador a
left join #num_sup1			b	on a.num_doc=b.num_doc and a.seguro=b.seguro and a.fecha_nac=b.fecha_nac and a.ubigeo=b.ubigeo 
left join #num_hb_sup1_3m	c	on a.num_doc=c.num_doc and a.seguro=c.seguro and a.fecha_nac=c.fecha_nac and a.ubigeo=c.ubigeo 
left join #num_sup_cont		d	on a.num_doc=d.num_doc and a.seguro=d.seguro and a.fecha_nac=d.fecha_nac and a.ubigeo=d.ubigeo 	
left join #num_Hb_12m		e	on a.num_doc=e.num_doc and a.seguro=e.seguro and a.fecha_nac=e.fecha_nac and a.ubigeo=e.ubigeo 


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
if object_id(N'FED25.TRAMAHIS_FED2024_25_SI_0203_Nominal_2025', N'U') is not null drop table FED25.TRAMAHIS_FED2024_25_SI_0203_Nominal_2025;
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
into FED25.TRAMAHIS_FED2024_25_SI_0203_Nominal_2025
from #tabla_reporte a
inner join MaeUbigeo_20240808 b on convert(int,a.ubigeo)=convert(int,b.ubigeo)

-- Tabla: Reporte Consolidado
if object_id(N'FED25.TRAMAHIS_FED2024_25_SI_0203_Consolidado_2025', N'U') is not null drop table FED25.TRAMAHIS_FED2024_25_SI_0203_Consolidado_2025;
select periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, seguro, sexo
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
, sum(denominador)		denominador
, sum(numerador)		numerador
, count(*)				Niños
, sum(den_hb)			den_hb
, sum(den_dx)			den_dx
---- Totales
, sum(num_sup1)				num_sup1		
, sum(num_hb_sup1_3m)		num_hb_sup1_3m			
, sum(num_sup_cont)			num_sup_cont		
, sum(num_sup_TA)			num_sup_TA			
-- Polimaltosado
, sum(num_sup1_po)			num_sup1_po			
, sum(num_hb_sup1_po_3m)	num_hb_sup1_po_3m		
, sum(num_sup2_po)			num_sup2_po				
, sum(num_sup_po_TA)		num_sup_po_TA			
-- Multimicro
, sum(num_sup1_multi)		num_sup1_multi			
, sum(num_hb_sup1_multi_3m)	num_hb_sup1_multi_3m	
, sum(num_sup2_multi)		num_sup2_multi			
, sum(num_sup3_multi)		num_sup3_multi			
, sum(num_sup4_multi)		num_sup4_multi			
, sum(num_sup5_multi)		num_sup5_multi			
, sum(num_sup6_multi)		num_sup6_multi			
, sum(num_sup_multi_TA)		num_sup_multi_TA		
-- TA
, sum(num_ta)				num_ta	
-- Hb 12m
, sum(num_hb_12m)			num_hb_12m		
into FED25.TRAMAHIS_FED2024_25_SI_0203_Consolidado_2025
-- into #TRAMAHIS_FED2024_25_SI_0203_Consolidado
from FED25.TRAMAHIS_FED2024_25_SI_0203_Nominal_2025
-- from #TRAMAHIS_FED2024_25_SI_0203_Nominal
group by periodo, año, mes, ubigeo_inei, diresa, departamento, provincia, distrito, seguro, sexo
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
------------------------------------------------------- =)
-- select * from #TRAMAHIS_FED2024_25_SI_0203_Consolidado
select * from FED25.TRAMAHIS_FED2024_25_SI_0203_Consolidado_2025