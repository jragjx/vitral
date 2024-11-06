#!/usr/bin/python
from time import time
from Parametros.lib_vitral import fColors
from Parametros.lib_vitral import FuncionesComunes as FC
from Parametros.lib_vitral import AlgoritmosProyeccion as AP
from Modelos.Modelos import ModelosPrediccion as MP

vInicio = time()

vCliente = 'Chiesi'
vDAASSismed = 'C:/DAAS/02.daas_sismed/PARQUET/'
vDAASMipres = 'C:/DAAS/01.daas_mipres_v2/'
vCarpetaCliente = 'C:/QsGJX/05.DesarrollosClientes/32.Chiesi/Vitral/'
vUsarIndicaciones = 0
vFechaInicioProyectar = '2024-01-01'
vUsarCUMMedicamentoGrupo = 0

# Utilizar la funcion fCrearBaseParaProyectar para generar los datos base de Sismed --> Los que se van a proyectar.
#FC.fCrearBaseParaProyectar(vCliente, vDAASSismed, 201901, vCarpetaCliente, 'INSTITUCIONAL', 'TODAS', vUsarIndicaciones, vUsarCUMMedicamentoGrupo)
AP.AlgoritmoArima(vCliente, vCarpetaCliente, vUsarIndicaciones, vFechaInicioProyectar, FC.fCalcularMesesAProyectar(vFechaInicioProyectar) )
AP.LlevarDatosAMipres(vCliente, vCarpetaCliente, 'ARIMA', vDAASMipres)

# Obtener estadisticas de tiempo y finalizar script.
FC.fCalcularTiempoEjecucionScript(vInicio)
quit()