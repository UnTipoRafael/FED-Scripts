--use [BD_HISINDICADORES]
--select  count( * )from [dbo].[TRAMAJESUS] where Edad_Reg='03' and Fecha_Registro='2025'


USE [BD_HISINDICADORES];

SELECT  Id_Paciente, Edad_Reg, Anio_Actual_Paciente,
	Mes_Actual_Paciente,Dia_Actual_Paciente,Id_Centro_Poblado,
	id_tipedad_reg
FROM [dbo].[TRAMAJESUS]
WHERE 
--    (Fecha_Nacimiento_Paciente LIKE '2023%' OR
--	Fecha_Nacimiento_Paciente LIKE '2024%') and
	Fecha_Registro LIKE '2025%' and
	Edad_Reg='3' and
	id_tipedad_reg ='A'
	group by  Id_Paciente, Edad_Reg,
	Anio_Actual_Paciente,Mes_Actual_Paciente,Dia_Actual_Paciente,
	Id_Centro_Poblado,id_tipedad_reg