#!/usr/bin/python
from time import time
from Parametros.lib_vitral import fColors
from Parametros.lib_vitral import FuncionesComunes as FC
from Parametros.lib_vitral import AlgoritmosProyeccion as AP

vInicio = time()

vCliente = 'Roche'
vDAASSismed = 'C:/DAAS/02.daas_sismed_v2/PARQUET/'
vDAASMipres = 'C:/DAAS/01.daas_mipres_v2/'
vCarpetaCliente = 'C:/QsGJX/05.DesarrollosClientes/28.Roche/Vitral/'
vFechaInicioProyectar = '2024-07-01'
vUsarCUMMedicamentoGrupo = 1

# Utilizar la funcion fCrearBaseParaProyectar para generar los datos base de Sismed --> Los que se van a proyectar.
#FC.fCrearBaseParaProyectar(vCliente, vDAASSismed, 202001, vCarpetaCliente, 'INSTITUCIONAL', 'TODAS', vUsarCUMMedicamentoGrupo)

## Suavizar tendencia media movil
#AP.RellenarVacios(vCliente, vCarpetaCliente, vUsarCUMMedicamentoGrupo)

## Encontrar atipicos
#AP.ProcesarOutliers(vCliente, vCarpetaCliente, vUsarCUMMedicamentoGrupo, 'MediaMovil')

## Arima JR
#AP.AlgoritmoArima(vCliente, vCarpetaCliente, vUsarCUMMedicamentoGrupo, vFechaInicioProyectar, FC.fCalcularMesesAProyectar(vFechaInicioProyectar), 'Outliers')

AP.LlevarDatosAMipres(vCliente, vCarpetaCliente, 'ARIMA', vDAASMipres, vUsarCUMMedicamentoGrupo)

# Obtener estadisticas de tiempo y finalizar script.
FC.fCalcularTiempoEjecucionScript(vInicio)
quit()