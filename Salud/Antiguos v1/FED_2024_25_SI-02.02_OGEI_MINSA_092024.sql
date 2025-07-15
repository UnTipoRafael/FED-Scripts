/*=======================================================================================================================================
Compromiso Gestion: SI-02 | Ni�as y ni�os de seis (06) meses de edad que reciben tratamiento con hierro o suplementaci�n 
							preventiva con hierro hasta los 12 meses de edad.

Indicador	:	SI-02.02  |	Porcentaje de ni�as y ni�os de seis (06) meses de edad, con diagn�stico anemia
							, que reciben tres (03) dosajes de hemoglobina y culminan el tratamiento con hierro a los 12 meses de edad

Elaboraci�n	:	MINISTERIO DESARROLLO E INCLUSION SOCIAL (MIDIS)
				DIRECCI�N DE SEGUIMIENTO Y EVALUACI�N (DGSE)
				DIRECCI�N DE SEGUIMIENTO (DS)
				/ JHONATAN LAVI
Revisi�n	:	MINISTERIO DE SALUD (MINSA)
				OFICINA GENERAL DE TECNOLOGIAS DE LA INFORMACION (OGTI)
				OFICINA DE GESTION DE LA INFORMACION (OGEI)
				Wilson URVIOLA ZAPATA - ANALISTA ESTADISTICO
Fecha		:	19/08/2024
============================================================================================================================================ */

-- Especificar la Base de datos a utilizar y en donde se guarda la informaci�n. 
use BD_HISINDICADORES
go

/* **********************************************************
				Tablas.

En esta parte se procede a seleccionar las Tablas/tramas/Base de datos 
utilizadas para generar el indicador. Las Tablas/tramas/Base de datos utilizados 
para este indicador son:

- 1.HIS MINSA (Mes de evalucaci�n)			| Cambia mes a mes.
- 2.HIS MINSA (A�o Previo)
- 3.Padron Nominal (Mes de Evaluaci�n)		| Cambia mes a mes.
************************************************************* */

-- 1.Tabla de datos : HIS - MINSA (Mes de evalucaci�n)
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
					'99199.17','99199.11',					--	c�digos de Entrega de Hierro (Tratamiento de anemia).
					'85018','85018.01',						--	c�digos de Dosaje de Hb.
					'D500','D508','D509','D649','D539'		--	C�digos de Diagnostico de anemia.
					)
and sw=1					-- Validaci�n del numero de documento.
and id_tipo_doc in (1,6)	-- Tipo de documento: DNI o CNV.


-- 2.Tabla de datos : HIS MINSA (A�o Previo)
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
					'99199.17','99199.11',					--	c�digos de Entrega de Hierro (Tratamiento de anemia).
					'85018','85018.01',						--	c�digos de Dosaje de Hb.
					'D500','D508','D509','D649','D539'		--	C�digos de Diagnostico de anemia.
					)
and sw=1					-- Validaci�n del numero de documento.
and id_tipo_doc in (1,6)	-- Tipo de documento: DNI o CNV.


-- 3.Tabla de datos : Padron Nominal (Mes de evaluaci�n)
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
la busqueda de informaci�n del indicador y en @mes_final el mes donde
finaliza la busqueda de informaci�n (Periodo de evaluaci�n).
*************************************************************************** */
if Object_id(N'tempdb..#tabla_reporte',N'U') is not null drop table #tabla_reporte;
create table #tabla_reporte
(
a�o						int,
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
fecha_trat1				date,
num_trat1				int,
fecha_hb_trat1_30d		date,
num_hb_trat1_30d		int,
fecha_trat2				date,
num_trat2				int,
fecha_trat3				date,
num_trat3				int,
fecha_TA				date,
num_TA					int,
fecha_Hb_12m			date,
num_Hb_12m				int
) 
	
declare @mes_inicio int, 
		@mes_eval int, 
		@a�o int 

set @a�o=2024
set @mes_inicio=1 
set @mes_eval=9 --< Modificar segun mes de evaluaci�n.

while @mes_inicio <= @mes_eval
begin

--%%%%%%%%%%%%%%%%
-- 1.DENOMINADOR
--%%%%%%%%%%%%%%%%

/* 1.1 -	Ni�as y ni�os de 12 meses 29 d�as de edad (394 d�as), en el mes de medici�n, registrados en el padr�n nominal con DNI 
			y CNV en l�nea, tipo de seguro MINSA */
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
where year(dateadd(dd,394,fecha_nac))=@a�o
and month(dateadd(dd,394,fecha_nac))=@mes_inicio					-- cumplen 12 meses 29 d�as de edad (394 d�as) en el mes de medici�n.


/* 1.2 -	Ni�as y Ni�os del denominador que Reciben un (01) dosaje de hemoglobina a los seis (06) meses de edad (se busca el dato entre 170-209 dias)
		, registrado con los c�digos: 85018 o 85018.01 (tipo de diagn�stico definitivo "D") */
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


/* 1.3 - Ni�as y Ni�os del denominador que Cuentan con diagn�stico de anemia registrado a los seis (06) meses de edad (se busca el dato entre 170-209 dias) con CIE-10: D500, D508, D509, D649 
			o D539 (tipo de diagn�stico definitivo "D"), a partir del primer dosaje de hemoglobina */ 
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

	
/* 1.4 - Uni�n de Informaci�n para armar el denominador del indicador. */
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
, iif(b.den_hb=1 and c.den_dx=1,1,0) denominador						-- Ni�o/as con diagn�stico de anemia
into #denominador
from #padron_den a
left join #padron_hb		b	on a.num_doc=b.num_doc and a.seguro=b.seguro and a.fecha_nac=b.fecha_nac and a.ubigeo=b.ubigeo and a.sexo=b.sexo
left join #padron_Dx		c	on a.num_doc=c.num_doc and a.seguro=c.seguro and a.fecha_nac=c.fecha_nac and a.ubigeo=c.ubigeo and a.sexo=c.sexo

--%%%%%%%%%%%%%%%%
-- 2.NUMERADOR
--%%%%%%%%%%%%%%%%

/* 2.1 -	Ni�as y ni�os del denominador, registrados que Reciben un (01) frasco de hierro hasta 7 d�as a partir del primer diagn�stico de anemia, registrados con c�digos 99199.17 o 99199.11. 
	// El registro debe estar vinculados a un c�digo CIE-10 de anemia D500, D508, D509, D649 o D539 (tipo de diagn�stico definitivo "D" o tipo de diagn�stico repetitivo "R") en la misma cita.
	// La evaluaci�n de las entregas de hierro es independientemente del LAB.
*/
if Object_id(N'tempdb..#num_trat1',N'U') is not null drop table #num_trat1;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_trat1,
		num_trat1=1
into #num_trat1
from #denominador	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where a.denominador=1														-- Ni�os del denominador.
and b.cod_item in ('99199.17','99199.11')									-- Entrega de Hierro.
and (b.fecha_atencion between a.fecha_dx and dateadd(dd,7,a.fecha_dx))		-- Entrega hasta 7 d�as a partir del primer diagn�stico de anemia
and b.id_cita in (	select distinct id_cita from #his_minsa	
					where cod_item in ('D500','D508','D509','D649','D539') and tipo_dx in ('D','R') )	-- El Registro debe estar vinculados a un c�digo CIE-10 de anemia D500, D508, D509, D649 o D539 (tipo de diagn�stico definitivo "D" o tipo de diagn�stico repetitivo "R")
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


/* 2.2 -	Ni�as y ni�os del denominador, registrados que Reciben un (01) dosaje de hemoglobina de control al mes de iniciado el tratamiento (se busca el dato entre 30 a 59 d�as)
			, regitrados con c�digos 85018 o 85018.01 (tipo de diagn�stico definitivo "D").
*/
if Object_id(N'tempdb..#num_hb_trat1_30d',N'U') is not null drop table #num_hb_trat1_30d;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		max(b.fecha_atencion) fecha_hb_trat1_30d,
		num_hb_trat1_30d=1
into #num_hb_trat1_30d
from #num_trat1	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where b.cod_item in ('85018','85018.01') and b.tipo_dx='D'										-- Dosaje de Hb.
and (b.fecha_atencion between dateadd(dd,30,a.fecha_trat1) and dateadd(dd,59,a.fecha_trat1) )	-- Entrega al mes de iniciado el tratamiento (se busca el dato entre 30 a 59 d�as).
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


/* 2.3 - Ni�as y ni�os del denominador, que Reciben al menos tres (03) frascos de hierro (incluye la entrega de hierro de inicio de tratamiento), registrados con c�digos 99199.17 o 99199.11, y que 
		culmina con un (01) registro TA a los seis (06) meses de iniciado el tratamiento (se busca el dato entre 170-209 dias), registrados con c�digo 99199.17 + 
		lab:TA o 99199.11 + lab:TA.

	// En los casos que existan de 02 o m�s entregas de hierro, se eval�a que cada entrega tenga un intervalo entre 25 a 70 d�as.
	// Los puntos 1 y 3 deben estar vinculados a un c�digo CIE-10 de anemia D500, D508, D509, D649 o D539 (tipo de diagn�stico definitivo "D" o tipo de diagn�stico repetitivo "R") en la misma cita.
	// La evaluaci�n de las entregas de hierro es independientemente del LAB.
*/

-- 2.3.1 - Cuenta con (02) Segunda Entrega de hierro. 
if Object_id(N'tempdb..#num_trat2',N'U') is not null drop table #num_trat2;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		min(b.fecha_atencion) fecha_trat2,
		num_trat2=1
into #num_trat2
from #num_trat1	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where b.cod_item in ('99199.17','99199.11')															-- Entrega de Hierro.
and (b.fecha_atencion between dateadd(dd,25,a.fecha_trat1) and dateadd(dd,70,a.fecha_trat1) )		-- Entrega tenga un intervalo entre 25 a 70 d�as.
and b.id_cita in (	select distinct id_cita from #his_minsa	
					where cod_item in ('D500','D508','D509','D649','D539') and tipo_dx in ('D','R') )	-- El Registro debe estar vinculados a un c�digo CIE-10 de anemia D500, D508, D509, D649 o D539 (tipo de diagn�stico definitivo "D" o tipo de diagn�stico repetitivo "R")
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


-- 2.3.2 - Cuenta con (03) Tercera Entrega de hierro. 
if Object_id(N'tempdb..#num_trat3',N'U') is not null drop table #num_trat3;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		min(b.fecha_atencion) fecha_trat3,
		num_trat3=1
into #num_trat3
from #num_trat2	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where b.cod_item in ('99199.17','99199.11')															-- Entrega de Hierro.
and (b.fecha_atencion between dateadd(dd,25,a.fecha_trat2) and dateadd(dd,70,a.fecha_trat2) )		-- Entrega tenga un intervalo entre 25 a 70 d�as.
and b.id_cita in (	select distinct id_cita from #his_minsa	
					where cod_item in ('D500','D508','D509','D649','D539') and tipo_dx in ('D','R') )	-- El Registro debe estar vinculados a un c�digo CIE-10 de anemia D500, D508, D509, D649 o D539 (tipo de diagn�stico definitivo "D" o tipo de diagn�stico repetitivo "R")
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


-- 2.3.3 - Culmina Entrega de Hierro con un (01) registro TA, Registrado a los seis (06) meses de iniciado el tratamiento (se busca el dato entre 170-209 dias).
if Object_id(N'tempdb..#num_TA',N'U') is not null drop table #num_TA;
select	a.num_doc,
		a.fecha_nac,
		a.seguro,
		a.ubigeo,
		min(b.fecha_atencion) fecha_TA,
		num_TA=1
into #num_TA
from #num_trat1	a
inner join #his_minsa	b on a.num_doc=b.num_doc
where  b.cod_item in ('99199.17','99199.11')	and b.valor_lab='TA'								-- Entrega de Hierro que culmina con un (01) registro TA
and (b.fecha_atencion between dateadd(dd,170,a.fecha_trat1) and dateadd(dd,209,a.fecha_trat1) )		-- Registrado a los seis (06) meses de iniciado el tratamiento (se busca el dato entre 170-209 dias)
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo


/* 2.4 -	Ni�as y ni�os del denominador, registrados Reciben un (01) dosaje de hemoglobina a los 12 meses de edad (se busca el dato entre entre 365 d�as a 394 d�as)
			, registrados con c�digos: 85018 o 85018.01 (tipo de diagn�stico definitivo "D")
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
where a.denominador=1																			-- Ni�os del denominador.
and b.cod_item in ('85018','85018.01') and b.tipo_dx='D'										-- Dosaje de Hb.
and (b.fecha_atencion between dateadd(dd,365,a.fecha_nac) and dateadd(dd,394,a.fecha_nac) )		-- Entrega a los 12 meses de edad (se busca el dato entre entre 365 d�as a 394 d�as)
group by a.num_doc,	a.fecha_nac, a.seguro, a.ubigeo
		

--%%%%%%%%%%%%%%%%
--	3.INDICADOR
--%%%%%%%%%%%%%%%% 

-- 3.1 Reporte de union del denominador y numerador. 
insert into #tabla_reporte
select @a�o a�o
, @mes_inicio mes
, a.tipo_doc
, a.num_doc
, a.fecha_nac
, a.sexo
, a.ubigeo
, a.seguro
, a.fecha_hb
, isnull(a.den_hb,0) den_hb
, a.fecha_dx
, isnull(a.den_Dx,0) den_dx
, a.denominador
, iif(b1.num_trat1=1 and b2.num_hb_trat1_30d=1 and b3.num_trat2=1 and b4.num_trat3=1 and b5.num_TA=1 and b6.num_Hb_12m=1,1,0) numerador
, b1.fecha_trat1				, isnull(b1.num_trat1,0)		num_trat1
, b2.fecha_hb_trat1_30d			, isnull(b2.num_hb_trat1_30d,0) num_hb_trat1_30d
, b3.fecha_trat2				, isnull(b3.num_trat2,0)		num_trat2
, b4.fecha_trat3				, isnull(b4.num_trat3,0)		num_trat3
, b5.fecha_TA					, isnull(b5.num_TA,0)			num_TA
, b6.fecha_Hb_12m				, isnull(b6.num_Hb_12m,0)		num_Hb_12m
from #denominador a
left join #num_trat1				b1	on a.seguro=b1.seguro and a.num_doc=b1.num_doc and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Primera entrega de Hierro
left join #num_hb_trat1_30d			b2	on a.seguro=b2.seguro and a.num_doc=b2.num_doc and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo				-- Dosaje a los 30d de la Primera entrega de Hierro
left join #num_trat2				b3	on a.seguro=b3.seguro and a.num_doc=b3.num_doc and a.fecha_nac=b3.fecha_nac and a.ubigeo=b3.ubigeo				-- Segunda entrega de Hierro
left join #num_trat3				b4	on a.seguro=b4.seguro and a.num_doc=b4.num_doc and a.fecha_nac=b4.fecha_nac and a.ubigeo=b4.ubigeo				-- Tercera entrega de Hierro
left join #num_TA					b5	on a.seguro=b5.seguro and a.num_doc=b5.num_doc and a.fecha_nac=b5.fecha_nac and a.ubigeo=b5.ubigeo				-- Culmina entrega de Hierro (TA)
left join #num_Hb_12m				b6	on a.seguro=b6.seguro and a.num_doc=b6.num_doc and a.fecha_nac=b6.fecha_nac and a.ubigeo=b6.ubigeo				-- Dosaje a los 12 meses de edad

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
	, si es febrero del otro a�o entonces 2024_02)
*************************************************************************** */

-- Tabla: REPORTE NOMINAL
if object_id(N'FED25.TRAMAHIS_FED2024_25_SI_0202_Nominal', N'U') is not null drop table FED25.TRAMAHIS_FED2024_25_SI_0202_Nominal;
select convert(char(4),a�o)+right('00'+convert(nvarchar(2),mes),2) Periodo
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
into FED25.TRAMAHIS_FED2024_25_SI_0202_Nominal
from #tabla_reporte a
inner join MaeUbigeo_20240808 b on convert(int,a.ubigeo)=convert(int,b.ubigeo)

-- Tabla: Reporte Consolidado
select periodo, a�o, mes, ubigeo_inei, diresa, departamento, provincia, distrito, seguro, sexo
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
, sum(denominador)		denominador
, sum(numerador)		numerador
, count(*)				Ni�os
, sum(den_hb)			den_hb
, sum(den_dx)			den_dx
, sum(num_hb_trat1_30d)	num_hb_trat1_30d
, sum(num_trat1)		num_trat1
, sum(num_trat2)		num_trat2
, sum(num_trat3)		num_trat3
, sum(num_TA)			num_TA
, sum(num_Hb_12m)		num_Hb_12m
from FED25.TRAMAHIS_FED2024_25_SI_0202_Nominal
group by periodo, a�o, mes, ubigeo_inei, diresa, departamento, provincia, distrito, seguro, sexo
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
------------------------------------------------------- =)
