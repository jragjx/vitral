#!/usr/bin/python
import pandas
from time import time, sleep
from os import listdir
from io import BytesIO
from shutil import move
from statsmodels.tsa.arima.model import ARIMA
from datetime import datetime
from sklearn.metrics import mean_squared_error, r2_score
import math
import numpy

##Librerias para envio Email
import smtplib
import ssl
import email.utils
from email.mime.text import MIMEText

#################################################################################################################################
## Funcion base para crear rutas a las carpetas que correspondan ################################################################
#################################################################################################################################

class fColors:
    HEADER = '\033[95m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    UNDERLINE = '\033[4m'

####################################################################################
# Función única para envío de emails, luego se llama la función ####################
####################################################################################

class EnvioEmail:

    def fEnvioEmails(mail_destino, nombre_destino, texto_email):

        msg = MIMEText(texto_email)
        msg.set_unixfrom('author')
        msg['To'] = email.utils.formataddr((nombre_destino, mail_destino))
        msg['From'] = email.utils.formataddr(('GJX S.A.S.', 'jrivas8812@gmail.com'))
        msg['Subject'] = 'GJX :: Falla en Extracción Transparencia Web Medicamentos'

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.set_debuglevel(False)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login('jrivas8812@gmail.com', 'fxaweakdtnvhpkfx')
        server.sendmail('jrivas8812@gmail.com', mail_destino, msg.as_string())
        server.quit()

class AlgoritmosProyeccion:

    def ProcesarOutliers(CLIENTE, CARPETA_PARAMETROS_CLIENTE, USAR_GRUPO_MEDICAMENTO, TIPO_CANTIDAD):

        # Dataframe Resultante
        df_SismedBaseOutliers = pandas.DataFrame()

        #Leer el dataframe con los datos extraídos de Sismed
        df_DataOrigen = pandas.read_csv(f'{CARPETA_PARAMETROS_CLIENTE}/Sismed_BaseMediaMovil_{CLIENTE}.csv', sep=";", encoding="utf-8")

        ## Listar los principios activos o medicamentos para hacer los ciclos de completar datos
        df_ListadoPrincipios = df_DataOrigen['NombrePrincipioActivo']
        df_ListadoPrincipios = df_ListadoPrincipios.drop_duplicates(ignore_index=True)  

        ## Obtener cantidades de cada medicamento/principio
        for i in df_ListadoPrincipios.index:

            print(f'\n\t{fColors.WARNING}Procesando Principio > {df_ListadoPrincipios[i]}{fColors.ENDC}')

            if USAR_GRUPO_MEDICAMENTO == 1:

                df_ListadoGrupoMed = df_DataOrigen['GrupoCUMMedicamento'].where(df_DataOrigen.NombrePrincipioActivo == df_ListadoPrincipios[i])
                df_ListadoGrupoMed = df_ListadoGrupoMed.drop_duplicates(ignore_index=True).dropna(how = 'all')

                for j in df_ListadoGrupoMed.index:

                    print(f'\t\t{fColors.OKGREEN}GrupoMed > {df_ListadoGrupoMed[j]}{fColors.ENDC}')

                    ## Obtener los periodos de la tendencia por Principio y GrupoMed
                    df_DatosDelMed = df_DataOrigen.where((df_DataOrigen.NombrePrincipioActivo == df_ListadoPrincipios[i]) & (df_DataOrigen.GrupoCUMMedicamento == df_ListadoGrupoMed[j]))
                    df_DatosDelMed = df_DatosDelMed.dropna(how = 'all')

                    df_SismedBaseOutliers = pandas.concat([df_SismedBaseOutliers, FuncionesComunes.fAplicarOutliersATendencia(df_DatosDelMed, TIPO_CANTIDAD)])

            if USAR_GRUPO_MEDICAMENTO == 0:

                df_DatosDelMed = df_DataOrigen.where(df_DataOrigen.NombrePrincipioActivo == df_ListadoPrincipios[i])
                df_DatosDelMed = df_DatosDelMed.dropna(how = 'all')

                df_SismedBaseOutliers = pandas.concat([df_SismedBaseOutliers, FuncionesComunes.fAplicarOutliersATendencia(df_DatosDelMed, TIPO_CANTIDAD)])
            
        df_SismedBaseOutliers.to_csv(f'{CARPETA_PARAMETROS_CLIENTE}/Sismed_BaseMediaMovil_SinOutliers_{CLIENTE}.csv', sep=';', index=None)

    def RellenarVacios(CLIENTE, CARPETA_PARAMETROS_CLIENTE, USAR_GRUPO_MEDICAMENTO):

        # Dataframe Resultante
        df_SismedBaseMediaMovil = pandas.DataFrame()
        
        #Leer el dataframe con los datos extraídos de Sismed
        df_DataOrigen = pandas.read_csv(f'{CARPETA_PARAMETROS_CLIENTE}/Sismed_Base_{CLIENTE}.csv', sep=";", encoding="utf-8")

        ## Listar los principios activos o medicamentos para hacer los ciclos de completar datos

        df_ListadoPrincipios = df_DataOrigen['NombrePrincipioActivo']
        df_ListadoPrincipios = df_ListadoPrincipios.drop_duplicates(ignore_index=True)            

        ## Obtener cantidades de cada medicamento/principio
        for i in df_ListadoPrincipios.index:

            print(f'\t{fColors.WARNING}Procesando Principio > {df_ListadoPrincipios[i]}{fColors.ENDC}')

            if USAR_GRUPO_MEDICAMENTO == 1:

                df_ListadoGrupoMed = df_DataOrigen['GrupoCUMMedicamento'].where(df_DataOrigen.NombrePrincipioActivo == df_ListadoPrincipios[i])
                df_ListadoGrupoMed = df_ListadoGrupoMed.drop_duplicates(ignore_index=True).dropna(how = 'all')

                for j in df_ListadoGrupoMed.index:

                    print(f'\t\t{fColors.OKGREEN}GrupoMed > {df_ListadoGrupoMed[j]}{fColors.ENDC}')

                    ## Obtener los periodos de la tendencia por Principio y GrupoMed
                    df_DatosDelMed = df_DataOrigen.where((df_DataOrigen.NombrePrincipioActivo == df_ListadoPrincipios[i]) & (df_DataOrigen.GrupoCUMMedicamento == df_ListadoGrupoMed[j]))
                    df_DatosDelMed = df_DatosDelMed.dropna(how = 'all')

                    df_SismedBaseMediaMovil = pandas.concat([df_SismedBaseMediaMovil, FuncionesComunes.fAplicarMediaMovilATendencia(df_DatosDelMed)])
                    del df_DatosDelMed                   

                    #vMediaMovil = df_DatosDelMed['Cantidad'].rolling(3).mean() <- La pensada inicialmente pero no aplica por la forma de los datos.

            elif USAR_GRUPO_MEDICAMENTO == 0:

                print(f'\t\t{fColors.OKGREEN}Principio Activo > {df_ListadoPrincipios[i]}{fColors.ENDC}')

                df_DatosDelMed = df_DataOrigen.where(df_DataOrigen.NombrePrincipioActivo == df_ListadoPrincipios[i])
                df_DatosDelMed = df_DatosDelMed.dropna(how = 'all')

                df_SismedBaseMediaMovil = pandas.concat([df_SismedBaseMediaMovil, FuncionesComunes.fAplicarMediaMovilATendencia(df_DatosDelMed)])
                del df_DatosDelMed

        ## Rellenar los vacios de las fechas que completó
        df_SismedBaseMediaMovil = df_SismedBaseMediaMovil.ffill()
        df_SismedBaseMediaMovil = df_SismedBaseMediaMovil.drop(['PeriodoNum'], axis=1)
        df_SismedBaseMediaMovil = df_SismedBaseMediaMovil.rename(columns={'PeriodoNumNuevos': 'PeriodoNum'})

        df_SismedBaseMediaMovil.to_csv(f'{CARPETA_PARAMETROS_CLIENTE}/Sismed_BaseMediaMovil_{CLIENTE}.csv', sep=';', index=None)

    def LlevarDatosAMipres(CLIENTE, CARPETA_PARAMETROS_CLIENTE, ALGORITMO_PREDICCION, DAAS_MIPRES):

        ################################################################################################################################################################################
        # Leer los datos de las predicciones y pasarlos a data en tablas con dimensiones PeriodoNum, NombrePrincipioActivo y Cantidad ##################################################
        ################################################################################################################################################################################
        # No funciona cuando se usan indicaciones, ya que no es una variable de Mipres, puede funcionar pero en Mipres armarlas manualmente. ###########################################
        
        df_datosBase = pandas.DataFrame()

        # 01.Leer los archivos con todas las moleculas que hayan pasado por la predicción
        print(f'\t{fColors.HEADER}***Leyendo archivo {CARPETA_PARAMETROS_CLIENTE}/Real_Prediccion_{ALGORITMO_PREDICCION}_{CLIENTE}.csv.{fColors.ENDC}')
        
        Tmp1_df_datosBase = pandas.read_csv(f'{CARPETA_PARAMETROS_CLIENTE}/Real_Prediccion_{ALGORITMO_PREDICCION}_{CLIENTE}.csv', sep=";", encoding="utf-8")
        Tmp1_df_datosBase['PeriodoNum'] = Tmp1_df_datosBase['PeriodoNum'].astype('datetime64[ns]')

        #Convertir las columnas que no sean PeriodoNum
        columnas_a_convertir = Tmp1_df_datosBase.columns[1:]
        
        # Itera sobre las columnas seleccionadas y convertirlas a tipo float
        for col in columnas_a_convertir:
            #Tmp1_df_datosBase[col] = Tmp1_df_datosBase[col].str.replace(',0','').astype(float)
            Tmp1_df_datosBase[col] = Tmp1_df_datosBase[col].replace('.0','').astype(float)

        for i in range(1, len(Tmp1_df_datosBase.columns)):
            # Convertir el nombre de la columna con la molecula a fila y acumular el archivo en df_datosBase
            vNombreColMed = Tmp1_df_datosBase.columns.values[i]

            Tmp2_df_datosBase = Tmp1_df_datosBase[['PeriodoNum', vNombreColMed]]
            Tmp2_df_datosBase['NombrePrincipioActivo'] = vNombreColMed
            Tmp2_df_datosBase = Tmp2_df_datosBase.rename(columns={vNombreColMed: 'Cantidad'})

            Tmp2_df_datosBase = Tmp2_df_datosBase.where(Tmp2_df_datosBase.Cantidad > 0)
            Tmp2_df_datosBase = Tmp2_df_datosBase.dropna(how = 'all')

            df_datosBase = pandas.concat([df_datosBase, Tmp2_df_datosBase])
            del Tmp2_df_datosBase

        # Guardar solo cuando se necesiten validar
        #df_datosBase.to_csv(f'{CARPETA_PARAMETROS_CLIENTE}/df_datosBase.csv', sep=';', index=None)
        del Tmp1_df_datosBase

        ################################################################################################################################################################################
        # Leer los datos necesarios de Mipres ##########################################################################################################################################
        ################################################################################################################################################################################

        print(f'\t{fColors.HEADER}***Leyendo datos Mipres DAAS y seleccionando info para modelo Vitral.{fColors.ENDC}')

        # Cargar las prescripciones y tipos de prescripcion de Mipres (Se cargan todas)
        df_MipresPrescrip = pandas.read_parquet(f'{DAAS_MIPRES}/Mipres_Prescripciones.parquet', engine='pyarrow', columns=['CodigoPrescripcion', 'DiagnosticoPrincipal', 'TipoRegimenAdministradora', 'GrupoAdministradora', 'GrupoEtareoQuinquenioPersona', 'Genero', 'Sede Prestador', 'Municipio Prestador', 'Departamento Prestador', 'EstadoRegistro'] )
        df_MipresPrescrip = df_MipresPrescrip.where(df_MipresPrescrip.EstadoRegistro == "ACTIVO")
        df_MipresPrescrip = df_MipresPrescrip.dropna(how = 'all')

        df_TipoPrescrip = pandas.read_parquet(f'{DAAS_MIPRES}/Mipres_TipoPrescripcion.parquet', engine='pyarrow', columns=['CodigoPrescripcion', 'TipoPrescripcion'] )
        df_TipoPrescrip = df_TipoPrescrip.where(df_TipoPrescrip.TipoPrescripcion == "MEDICAMENTO")
        df_TipoPrescrip = df_TipoPrescrip.dropna(how = 'all')

        # Reducir la cantidad de prescripciones con un inner join por tipo = MEDICAMENTO
        df_MipresPrescrip = df_MipresPrescrip.join(df_TipoPrescrip.set_index('CodigoPrescripcion'), on='CodigoPrescripcion', how='inner')
        del df_TipoPrescrip

        # Datos para limitar las moleculas de Mipres, se traen de df_datosBase punto 01.
        df_PrincipiosActivos = df_datosBase[['NombrePrincipioActivo']]
        df_PrincipiosActivos = df_PrincipiosActivos.drop_duplicates(ignore_index=True)

        # Reducir el numero de prescripciones de la tabla de medicamentos de Mipres
        df_MedPrescrip = pandas.read_parquet(f'{DAAS_MIPRES}/Mipres_Medicamento.parquet', engine='pyarrow', columns=['CodigoPrescripcion', 'CantidadTotalFormulada', 'NombrePrincipioActivo'] )
        df_MedPrescrip = df_MedPrescrip.rename(columns={"NombrePrincipioActivo": "NombrePrincipioActivoEnMipres"})
            
            ##Renombrar los principios activos para que se llamen igual a Sismed --- Porque sino, no lleva la predicción a nivel de Prescripción.
        df_NombresPrincipiosMipres = pandas.read_excel(f"{CARPETA_PARAMETROS_CLIENTE}/_MoleculasCliente.xlsx", engine='openpyxl', sheet_name="Hoja1", na_filter=True, usecols=['NombrePrincipioActivo', 'NombrePrincipioActivoEnMipres'])
        df_NombresPrincipiosMipres = df_NombresPrincipiosMipres.rename(columns={"NombrePrincipioActivo": "NombrePrincipioActivoDeSismed"})
        df_NombresPrincipiosMipres = df_NombresPrincipiosMipres.dropna(how = 'any')

        df_MedPrescrip = df_MedPrescrip.merge(df_NombresPrincipiosMipres, left_on='NombrePrincipioActivoEnMipres', right_on='NombrePrincipioActivoEnMipres', how='left')
        del df_NombresPrincipiosMipres

        df_MedPrescrip['NombrePrincipioActivo'] = numpy.where(df_MedPrescrip['NombrePrincipioActivoDeSismed'].isnull(), df_MedPrescrip['NombrePrincipioActivoEnMipres'], df_MedPrescrip['NombrePrincipioActivoDeSismed'])
        df_MedPrescrip = df_MedPrescrip.merge(df_PrincipiosActivos.set_index('NombrePrincipioActivo'), on='NombrePrincipioActivo', how='inner')
        df_MedPrescrip = df_MedPrescrip.drop(['NombrePrincipioActivoDeSismed', 'NombrePrincipioActivoEnMipres'], axis=1)
        
        # Unir las prescripciones (df_MipresPrescrip) con el medicamento (df_MedPrescrip) reduciendo valores por CodigoPrescripcion
        df_MipresPrescrip = df_MipresPrescrip.merge(df_MedPrescrip.set_index('CodigoPrescripcion'), on='CodigoPrescripcion', how='inner')
        del df_MedPrescrip

        # Eliminar campos innecesarios de la tabla MipresPrescrip
        df_MipresPrescrip = df_MipresPrescrip.drop(['TipoPrescripcion', 'EstadoRegistro', 'CodigoPrescripcion'], axis=1)

        # Agregacion para el campo CantidadTotalFormulada
        df_MipresPrescrip = df_MipresPrescrip.groupby(by=['DiagnosticoPrincipal', 'TipoRegimenAdministradora', 'GrupoAdministradora', 'GrupoEtareoQuinquenioPersona', 'Genero', 'Sede Prestador', 'Municipio Prestador', 'Departamento Prestador', 'NombrePrincipioActivo'], as_index=False).agg(CantidadTotalFormulada = ("CantidadTotalFormulada", "sum"))

        ################################################################################################################################################################################
        # Calcular los pesos porcentuales de Mipres para dar la temporalidad similar a la predicción ###################################################################################
        ################################################################################################################################################################################

        print(f'\t{fColors.HEADER}***Calculando la distribución de pesos porcentuales de las llaves de Mipres.{fColors.ENDC}')

        df_MipresPrescrip = df_MipresPrescrip.groupby(by=['DiagnosticoPrincipal', 'TipoRegimenAdministradora', 'GrupoAdministradora', 'GrupoEtareoQuinquenioPersona', 'Genero', 'Sede Prestador', 'Municipio Prestador', 'Departamento Prestador', 'NombrePrincipioActivo'], as_index=False).agg(CantidadTotalFormulada = ("CantidadTotalFormulada", "mean"))
        df_MipresPrescrip = df_MipresPrescrip.assign(KeyProyeccion = lambda x: x['DiagnosticoPrincipal'].index)
        
        df_Tmp1_AvgPrescrip_Llave = df_MipresPrescrip[['NombrePrincipioActivo', 'KeyProyeccion', 'CantidadTotalFormulada']]
        df_Tmp1_AvgPrescrip_Llave = df_Tmp1_AvgPrescrip_Llave.groupby(by=['KeyProyeccion', 'NombrePrincipioActivo'], as_index=False).agg(AvgPrescrip_Llave = ("CantidadTotalFormulada", "mean"))
        
        df_Tmp2_AvgPrescrip_Llave = df_Tmp1_AvgPrescrip_Llave.groupby(by=['NombrePrincipioActivo'], as_index=False).agg(CantLlaves = ('KeyProyeccion', 'count'), TotalPorMes= ('AvgPrescrip_Llave', 'sum'))

        df_PesosPorLlave = df_Tmp1_AvgPrescrip_Llave.merge(df_Tmp2_AvgPrescrip_Llave.set_index(['NombrePrincipioActivo']), on=['NombrePrincipioActivo'], how='left')
        del df_Tmp1_AvgPrescrip_Llave
        del df_Tmp2_AvgPrescrip_Llave

        df_PesosPorLlave['MS_Peso'] = df_PesosPorLlave['AvgPrescrip_Llave']/df_PesosPorLlave['TotalPorMes']

        # Guardar solo cuando se necesiten validar
        #df_PesosPorLlave.to_csv(f'{CARPETA_PARAMETROS_CLIENTE}/df_PesosPorLlave.csv', sep=';', index=None)

        ################################################################################################################################################################################
        # Aplicar los pesos a los datos de Mipres ######################################################################################################################################
        ################################################################################################################################################################################

        print(f'\t{fColors.HEADER}***Cruzado Mipres con Predicción de Sismed.{fColors.ENDC}')

        # A tener en cuenta:  df_datosBase-> tiene la proyección. df_MipresPrescrip-> tiene Mipres Agrupado con la Key. df_PesosPorLlave-> tiene los pesos porcentuales.
        SegmentoProyectado = pandas.DataFrame()

        # Recorrer el dataframe con las predicciones df_datosBase
        for i in range(len(df_datosBase)):

            # Quitar comentario solo para validar que va a procesar en el Join dentro del For.
            #print(f'\t{fColors.WARNING}Procesando >PeriodoNum {df_datosBase.iloc[i]['PeriodoNum']} >Principio Activo {df_datosBase.iloc[i]['NombrePrincipioActivo']} >Cantidad {df_datosBase.iloc[i]['Cantidad']}{fColors.ENDC}')

            # Seleccionar los datos de df_datosBase, osea el Mipres agrupado inicial
            df_Tmp1_SegmentoProyectado = df_MipresPrescrip.where(df_MipresPrescrip.NombrePrincipioActivo == df_datosBase.iloc[i]['NombrePrincipioActivo'])
            df_Tmp1_SegmentoProyectado = df_Tmp1_SegmentoProyectado.dropna(how = 'all')

            # Seleccionar los pesos por Key en un mes y molecula
            df_Tmp2_SegmentoProyectado = df_PesosPorLlave.where(df_PesosPorLlave.NombrePrincipioActivo == df_datosBase.iloc[i]['NombrePrincipioActivo'])
            df_Tmp2_SegmentoProyectado = df_Tmp2_SegmentoProyectado.dropna(how = 'all')
            df_Tmp2_SegmentoProyectado = df_Tmp2_SegmentoProyectado[['KeyProyeccion', 'MS_Peso']]

            Tmp_SegmentoProyectado = df_Tmp1_SegmentoProyectado.merge(df_Tmp2_SegmentoProyectado.set_index(['KeyProyeccion']), on='KeyProyeccion', how='left')

            # Agregar los datos del dataframe de llaves al nuevo
            Tmp_SegmentoProyectado['PeriodoNum_Proyectado'] = df_datosBase.iloc[i]['PeriodoNum']
            Tmp_SegmentoProyectado['ValorProyectado'] = df_datosBase.iloc[i]['Cantidad']
            Tmp_SegmentoProyectado['ProyectadoxPeso'] = Tmp_SegmentoProyectado['ValorProyectado'] * Tmp_SegmentoProyectado['MS_Peso']

            SegmentoProyectado = pandas.concat([SegmentoProyectado, Tmp_SegmentoProyectado])

            del Tmp_SegmentoProyectado
            del df_Tmp1_SegmentoProyectado
            del df_Tmp2_SegmentoProyectado

        
        # Guardar solo para validar
        SegmentoProyectado.to_csv(f'{CARPETA_PARAMETROS_CLIENTE}/SegmentoProyectado_{ALGORITMO_PREDICCION}_{CLIENTE}.csv', sep=';', index=None)
        SegmentoProyectado.to_parquet(f'{CARPETA_PARAMETROS_CLIENTE}/SegmentoProyectado_{ALGORITMO_PREDICCION}_{CLIENTE}.parquet', index=None, engine='pyarrow')

        # Borrar dataframes que ya no se van a usar.
        del df_MipresPrescrip
        del SegmentoProyectado
        del df_datosBase
        del df_PesosPorLlave

    def AlgoritmoArima(CLIENTE, CARPETA_PARAMETROS_CLIENTE, USAR_GRUPOMED, FECHA_PREDECIR_DESDE, PERIODOS_PREDECIR, TIPO_CANTIDAD):

        """
        Las variables de parametros de la funcion significan lo siguiente:
        CLIENTE                     -> Nombre del cliente, que se usará para nombrar los archivos resultantes.
        CARPETA_PARAMETROS_CLIENTE  -> Carpeta base donde queda todo lo del cliente que use el producto.
        FECHA_PREDECIR_DESDE        -> Fecha desde la cual se empezara a hacer el forecast.
        PERIODOS_PREDECIR           -> Cuantos periodos a futuro se van a predecir desde la fecha dada anterior.
        USAR_INDICACIONES           -> Usar el campo "Indicacion" en la fuente generada por la base sismed.
        TIPO_CANTIDAD               -> Usar la variable que se quiera Original ó MediaMovil
        """

        if USAR_GRUPOMED == 0:

            df_PDQ = pandas.DataFrame()

            ################################################################################################################################################################################
            # Convertir dataframe a formato que lo entienda ARIMA ##########################################################################################################################
            ################################################################################################################################################################################

            if TIPO_CANTIDAD == 'Original':
                df_datosBase = pandas.read_csv(f'{CARPETA_PARAMETROS_CLIENTE}/Sismed_BaseMediaMovil_{CLIENTE}.csv', sep=";", encoding="utf-8", index_col=False, usecols=['PeriodoNum', 'NombrePrincipioActivo', 'Cantidad'])
            elif TIPO_CANTIDAD == 'MediaMovil':
                df_datosBase = pandas.read_csv(f'{CARPETA_PARAMETROS_CLIENTE}/Sismed_BaseMediaMovil_{CLIENTE}.csv', sep=";", encoding="utf-8", index_col=False, usecols=['PeriodoNum', 'NombrePrincipioActivo', 'Cantidad_MediaMovil'])
                df_datosBase = df_datosBase.rename(columns={'Cantidad_MediaMovil': 'Cantidad'})
            elif TIPO_CANTIDAD == 'Outliers':
                df_datosBase = pandas.read_csv(f'{CARPETA_PARAMETROS_CLIENTE}/Sismed_BaseMediaMovil_SinOutliers_{CLIENTE}.csv', sep=";", encoding="utf-8", index_col=False, usecols=['PeriodoNum', 'NombrePrincipioActivo', 'Cantidad_AtipicosCorregidos'])
                df_datosBase = df_datosBase.rename(columns={'Cantidad_AtipicosCorregidos': 'Cantidad'})

            #Hacer un groupby para eliminar las duplicidades cuando se usa el filtro de productos. Ademas si el dataframe viene con GrupoMedicamento, no lo tiene en cuenta porque predice es la molecula.
            df_datosBase = df_datosBase.groupby(['PeriodoNum', 'NombrePrincipioActivo'])['Cantidad'].sum().reset_index()

            df_PrincipiosActivos = df_datosBase['NombrePrincipioActivo']
            df_PrincipiosActivos = df_PrincipiosActivos.drop_duplicates(ignore_index=True)

            # Dataframe con datos finales organizados
            df_Organizado = df_datosBase[['PeriodoNum']]
            df_Organizado = df_Organizado.drop_duplicates()

            for i in df_PrincipiosActivos.index:

                Tmp_df_Organizado = df_datosBase.where(df_datosBase.NombrePrincipioActivo == df_PrincipiosActivos[i])
                Tmp_df_Organizado = Tmp_df_Organizado.dropna(how = 'all')
                Tmp_df_Organizado = Tmp_df_Organizado[['PeriodoNum', 'Cantidad']]
                Tmp_df_Organizado = Tmp_df_Organizado.rename(columns={'Cantidad': df_PrincipiosActivos[i]})

                df_Organizado = df_Organizado.merge(Tmp_df_Organizado, on='PeriodoNum', how='left')
                del Tmp_df_Organizado

            # Formatear el campo PeriodoNum
            df_Organizado['PeriodoNum'] = pandas.to_datetime(df_Organizado['PeriodoNum'], format='%Y%m')
            df_Organizado.set_index('PeriodoNum', inplace=True)
                
            # Guardar solo cuando se necesiten validar
            #df_Organizado.to_csv(f'{CARPETA_PARAMETROS_CLIENTE}/df_Organizado.csv', sep=';', index=True)

            ################################################################################################################################################################################
            # Algoritmo ARIMA para predecir ################################################################################################################################################
            ################################################################################################################################################################################
            """
            * p (Autoregressive term): Es el número de términos autorregresivos. Controla la dependencia lineal entre una observación y sus valores retrasados (lag). 
                Un valor más alto de p permite que el modelo tenga en cuenta más valores pasados, lo que puede ser útil si los datos muestran una correlación con sus propios retrasos.
            * d (Integrated term): Es el número de veces que se diferencia la serie temporal para hacerla estacionaria. La diferenciación se refiere a la diferencia entre observaciones consecutivas (restar el valor en el tiempo t por el valor en el tiempo t-1). 
                Un valor más alto de d implica una diferenciación más profunda, lo que puede ser necesario para hacer que la serie temporal sea estacionaria.
            * q (Moving Average term): Es el tamaño de la ventana de la media móvil. Controla la dependencia lineal entre una observación y un error de predicción retrasado. 
                Un valor más alto de q permite que el modelo tenga en cuenta más errores de predicción pasados.
            * Uso -> order=(p,d,q)
            """

            df_Tmp_PrediccionesAcumulado = pandas.DataFrame()

            # Ajustar el modelo ARIMA para cada serie temporal (por molecula)
            for i in df_PrincipiosActivos.index:

                df_Tmp_ParaPredecir = df_Organizado[df_PrincipiosActivos[i]].dropna(how = 'all')
                
                print(f'{fColors.HEADER}Prediccion de -> {df_PrincipiosActivos[i]}{fColors.ENDC}')
                dates = pandas.date_range(start=FECHA_PREDECIR_DESDE, periods=PERIODOS_PREDECIR, freq='MS')

                # Iterar las variables pdq para saber cual es la mejor combinacion
                df_PDQ_Combinaciones = pandas.DataFrame()

                for p in range(1,7):
                    for d in range(1,7):
                        for q in range(1,7):
                            
                            try:
                                model = ARIMA(df_Tmp_ParaPredecir, order=(p, d, q)).fit()
                                forecast = model.forecast(steps=PERIODOS_PREDECIR)

                                # Crear un DataFrame para almacenar las predicciones
                                predictions = pandas.DataFrame({
                                    'PeriodoNum': dates,
                                    df_PrincipiosActivos[i]: forecast
                                })
                                predictions.set_index('PeriodoNum', inplace=True)

                                # Calcular MSE, RMSE y Porcentaje de Error
                                vCantReg = len(df_Tmp_ParaPredecir)
                                vCantRegPrediccion = len(predictions)

                                # Se hace este calculo para dejar del mismo tamaño el vector de datos reales vs. datos de prediccion
                                if vCantReg == vCantRegPrediccion:
                                    vLargoArreglo = vCantReg
                                    predictionsParaError = predictions
                                    del predictions
                                elif vCantReg < vCantRegPrediccion:
                                    vLargoArreglo = vCantReg
                                    predictionsParaError = predictions.head(vLargoArreglo)
                                    del predictions
                                elif vCantReg > vCantRegPrediccion:
                                    vLargoArreglo = vCantRegPrediccion
                                    predictionsParaError = predictions.head(vLargoArreglo)
                                    del predictions

                                r2 = r2_score(df_Tmp_ParaPredecir.tail(vLargoArreglo), predictionsParaError)
                                mse = mean_squared_error(df_Tmp_ParaPredecir.tail(vLargoArreglo), predictionsParaError)
                                rmse = math.sqrt(mse)
                                error = rmse/vCantReg

                                Tmp_df_PDQ_Combinaciones = pandas.DataFrame([[p, d, q, r2, mse, rmse, error]], columns=['P', 'D', 'Q', 'R2','MSE', 'RMSE', 'ERROR'])
                                df_PDQ_Combinaciones = pandas.concat([df_PDQ_Combinaciones, Tmp_df_PDQ_Combinaciones])

                                print(f'\n{fColors.UNDERLINE}Proceso ejecutado con:{fColors.ENDC}')
                                print(f'{fColors.WARNING}P:{p} D:{d} Q:{q} R2:{round(r2, 2)} MSE:{round(mse, 2)} RMSE:{round(rmse, 2)} ERROR:{round(error, 2)}{fColors.ENDC}\n')
                                
                                del Tmp_df_PDQ_Combinaciones
                                del predictionsParaError
                            
                            except:
                                pass

                #df_PDQ_Combinaciones.to_csv(f'{CARPETA_PARAMETROS_CLIENTE}/df_PDQ_Combinaciones_{df_PrincipiosActivos[i]}.csv', sep=';', index=False)

                # Obtener la combinacion con el menor error
                vListaR2 = df_PDQ_Combinaciones["R2"].to_list()
                vMasCercadoAUno = vListaR2[min(range(len(vListaR2)), key = lambda i: abs(vListaR2[i]-1))]

                print(f'{fColors.OKGREEN}Valor R2 más cercano a 1: {vMasCercadoAUno}{fColors.ENDC}')
                
                df_PDQ_Combinaciones = df_PDQ_Combinaciones.where(df_PDQ_Combinaciones.R2 == vMasCercadoAUno)
                df_PDQ_Combinaciones = df_PDQ_Combinaciones.dropna(how = 'all')
                df_PDQ_Combinaciones['NombrePrincipioActivo'] = df_PrincipiosActivos[i]

                df_PDQ = pandas.concat([df_PDQ, df_PDQ_Combinaciones])

                p = df_PDQ_Combinaciones['P'].iloc[0]
                d = df_PDQ_Combinaciones['D'].iloc[0]
                q = df_PDQ_Combinaciones['Q'].iloc[0]
                r2 = df_PDQ_Combinaciones['R2'].iloc[0]
                mse = df_PDQ_Combinaciones['MSE'].iloc[0]
                rmse = df_PDQ_Combinaciones['RMSE'].iloc[0]
                error = df_PDQ_Combinaciones['ERROR'].iloc[0]
                del df_PDQ_Combinaciones

                # Aplicar el mejor PDQ a la prediccion definitiva
                model = ARIMA(df_Tmp_ParaPredecir, order=(p, d, q)).fit()
                forecast = model.forecast(steps=PERIODOS_PREDECIR)

                predictions = pandas.DataFrame({
                    'PeriodoNum': dates,
                    df_PrincipiosActivos[i]: forecast
                })
                predictions.set_index('PeriodoNum', inplace=True)

                print(f'\t{fColors.WARNING}Mejor combinación PDQ: {p}, {d}, {q}.{fColors.ENDC}')
                print(f'\t{fColors.OKGREEN}R2: {round(r2, 2)}\tMSE: {round(mse, 2)}\tRMSE: {round(rmse, 2)}\tERROR: {round(error, 2)}{fColors.ENDC}')

                # Esperar solo para alcanzar a ver en pantalla los numeros.
                #sleep(10)
                
                del df_Tmp_ParaPredecir
                df_Tmp_PrediccionesAcumulado = pandas.concat([df_Tmp_PrediccionesAcumulado, predictions])

            df_Organizado = pandas.concat([df_Organizado, df_Tmp_PrediccionesAcumulado])
            del df_Tmp_PrediccionesAcumulado

            # Almacenar la tabla real+prediccion
            df_Organizado.to_csv(f'{CARPETA_PARAMETROS_CLIENTE}/Real_Prediccion_ARIMA_{CLIENTE}.csv', sep=';', index=True)
            df_PDQ.to_csv(f'{CARPETA_PARAMETROS_CLIENTE}/df_PDQ_ARIMA.csv', sep=';', index=True)

        elif USAR_GRUPOMED == 1:
            
            df_PDQ = pandas.DataFrame()

            ################################################################################################################################################################################
            # Convertir dataframe a formato que lo entienda ARIMA ##########################################################################################################################
            ################################################################################################################################################################################

            if TIPO_CANTIDAD == 'Original':
                df_datosBase = pandas.read_csv(f'{CARPETA_PARAMETROS_CLIENTE}/Sismed_BaseMediaMovil_{CLIENTE}.csv', sep=";", encoding="utf-8", index_col=False, usecols=['PeriodoNum', 'NombrePrincipioActivo', 'GrupoCUMMedicamento', 'Cantidad'])
            elif TIPO_CANTIDAD == 'MediaMovil':
                df_datosBase = pandas.read_csv(f'{CARPETA_PARAMETROS_CLIENTE}/Sismed_BaseMediaMovil_{CLIENTE}.csv', sep=";", encoding="utf-8", index_col=False, usecols=['PeriodoNum', 'NombrePrincipioActivo', 'GrupoCUMMedicamento', 'Cantidad_MediaMovil'])
                df_datosBase = df_datosBase.rename(columns={'Cantidad_MediaMovil': 'Cantidad'})
            elif TIPO_CANTIDAD == 'Outliers':
                df_datosBase = pandas.read_csv(f'{CARPETA_PARAMETROS_CLIENTE}/Sismed_BaseMediaMovil_SinOutliers_{CLIENTE}.csv', sep=";", encoding="utf-8", index_col=False, usecols=['PeriodoNum', 'NombrePrincipioActivo', 'GrupoCUMMedicamento', 'Cantidad_AtipicosCorregidos'])
                df_datosBase = df_datosBase.rename(columns={'Cantidad_AtipicosCorregidos': 'Cantidad'})

            #Hacer un groupby para eliminar las duplicidades cuando se usa el filtro de productos. Ademas si el dataframe viene con GrupoMedicamento, no lo tiene en cuenta porque predice es la molecula.
            df_datosBase = df_datosBase.groupby(['PeriodoNum', 'NombrePrincipioActivo', 'GrupoCUMMedicamento'])['Cantidad'].sum().reset_index()

            #Poner en una sola columna NombrePrincipioActivo + GrupoCUMMedicamento para individualizar los datos.
            df_datosBase['MedIndividual'] = df_datosBase['NombrePrincipioActivo'] + '_' + df_datosBase['GrupoCUMMedicamento']

            df_MedIndividualizados = df_datosBase['MedIndividual']
            df_MedIndividualizados = df_MedIndividualizados.drop_duplicates(ignore_index=True)

            # Dataframe con datos finales organizados
            df_Organizado = df_datosBase[['PeriodoNum']]
            df_Organizado = df_Organizado.drop_duplicates()

            for i in df_MedIndividualizados.index:

                Tmp_df_Organizado = df_datosBase.where(df_datosBase.MedIndividual == df_MedIndividualizados[i])
                Tmp_df_Organizado = Tmp_df_Organizado.dropna(how = 'all')
                Tmp_df_Organizado = Tmp_df_Organizado[['PeriodoNum', 'Cantidad']]
                Tmp_df_Organizado = Tmp_df_Organizado.rename(columns={'Cantidad': df_MedIndividualizados[i]})

                df_Organizado = df_Organizado.merge(Tmp_df_Organizado, on='PeriodoNum', how='left')
                del Tmp_df_Organizado

            # Formatear el campo PeriodoNum
            df_Organizado['PeriodoNum'] = pandas.to_datetime(df_Organizado['PeriodoNum'], format='%Y%m')
            df_Organizado.set_index('PeriodoNum', inplace=True)
                
            # Guardar solo cuando se necesiten validar
            df_Organizado.to_csv(f'{CARPETA_PARAMETROS_CLIENTE}/df_Organizado.csv', sep=';', index=True)

            ################################################################################################################################################################################
            # Algoritmo ARIMA para predecir ################################################################################################################################################
            ################################################################################################################################################################################
            """
            * p (Autoregressive term): Es el número de términos autorregresivos. Controla la dependencia lineal entre una observación y sus valores retrasados (lag). 
                Un valor más alto de p permite que el modelo tenga en cuenta más valores pasados, lo que puede ser útil si los datos muestran una correlación con sus propios retrasos.
            * d (Integrated term): Es el número de veces que se diferencia la serie temporal para hacerla estacionaria. La diferenciación se refiere a la diferencia entre observaciones consecutivas (restar el valor en el tiempo t por el valor en el tiempo t-1). 
                Un valor más alto de d implica una diferenciación más profunda, lo que puede ser necesario para hacer que la serie temporal sea estacionaria.
            * q (Moving Average term): Es el tamaño de la ventana de la media móvil. Controla la dependencia lineal entre una observación y un error de predicción retrasado. 
                Un valor más alto de q permite que el modelo tenga en cuenta más errores de predicción pasados.
            * Uso -> order=(p,d,q)
            """

            df_Tmp_PrediccionesAcumulado = pandas.DataFrame()

            # Ajustar el modelo ARIMA para cada serie temporal (por molecula)
            for i in df_MedIndividualizados.index:

                df_Tmp_ParaPredecir = df_Organizado[df_MedIndividualizados[i]].dropna(how = 'all')
                
                print(f'{fColors.HEADER}Prediccion de -> {df_MedIndividualizados[i]}{fColors.ENDC}')
                dates = pandas.date_range(start=FECHA_PREDECIR_DESDE, periods=PERIODOS_PREDECIR, freq='MS')

                # Iterar las variables pdq para saber cual es la mejor combinacion
                df_PDQ_Combinaciones = pandas.DataFrame()

                for p in range(1,6):
                    for d in range(1,6):
                        for q in range(1,6):
                            
                            try:
                                model = ARIMA(df_Tmp_ParaPredecir, order=(p, d, q)).fit()
                                forecast = model.forecast(steps=PERIODOS_PREDECIR)

                                # Crear un DataFrame para almacenar las predicciones
                                predictions = pandas.DataFrame({
                                    'PeriodoNum': dates,
                                    df_MedIndividualizados[i]: forecast
                                })
                                predictions.set_index('PeriodoNum', inplace=True)

                                # Calcular MSE, RMSE y Porcentaje de Error
                                vCantReg = len(df_Tmp_ParaPredecir)
                                vCantRegPrediccion = len(predictions)

                                # Se hace este calculo para dejar del mismo tamaño el vector de datos reales vs. datos de prediccion
                                if vCantReg == vCantRegPrediccion:
                                    vLargoArreglo = vCantReg
                                    predictionsParaError = predictions
                                    del predictions
                                elif vCantReg < vCantRegPrediccion:
                                    vLargoArreglo = vCantReg
                                    predictionsParaError = predictions.head(vLargoArreglo)
                                    del predictions
                                elif vCantReg > vCantRegPrediccion:
                                    vLargoArreglo = vCantRegPrediccion
                                    predictionsParaError = predictions.head(vLargoArreglo)
                                    del predictions

                                r2 = r2_score(df_Tmp_ParaPredecir.tail(vLargoArreglo), predictionsParaError)
                                mse = mean_squared_error(df_Tmp_ParaPredecir.tail(vLargoArreglo), predictionsParaError)
                                rmse = math.sqrt(mse)
                                error = rmse/vCantReg

                                Tmp_df_PDQ_Combinaciones = pandas.DataFrame([[p, d, q, r2, mse, rmse, error]], columns=['P', 'D', 'Q', 'R2','MSE', 'RMSE', 'ERROR'])
                                df_PDQ_Combinaciones = pandas.concat([df_PDQ_Combinaciones, Tmp_df_PDQ_Combinaciones])

                                print(f'\n{fColors.UNDERLINE}Proceso ejecutado con:{fColors.ENDC}')
                                print(f'{fColors.WARNING}P:{p} D:{d} Q:{q} R2:{round(r2, 2)} MSE:{round(mse, 2)} RMSE:{round(rmse, 2)} ERROR:{round(error, 2)}{fColors.ENDC}\n')
                                
                                del Tmp_df_PDQ_Combinaciones
                                del predictionsParaError
                            
                            except:
                                pass

                try:

                    # Obtener la combinacion con el menor error
                    vListaR2 = df_PDQ_Combinaciones["R2"].to_list()
                    vMasCercadoAUno = vListaR2[min(range(len(vListaR2)), key = lambda i: abs(vListaR2[i]-1))]

                    print(f'{fColors.OKGREEN}Valor R2 más cercano a 1: {vMasCercadoAUno}{fColors.ENDC}')
                    
                    df_PDQ_Combinaciones = df_PDQ_Combinaciones.where(df_PDQ_Combinaciones.R2 == vMasCercadoAUno)
                    df_PDQ_Combinaciones = df_PDQ_Combinaciones.dropna(how = 'all')
                    df_PDQ_Combinaciones['MedIndividual'] = df_MedIndividualizados[i]

                    print(df_PDQ_Combinaciones)

                    df_PDQ = pandas.concat([df_PDQ, df_PDQ_Combinaciones])

                    p = df_PDQ_Combinaciones['P'].iloc[0]
                    d = df_PDQ_Combinaciones['D'].iloc[0]
                    q = df_PDQ_Combinaciones['Q'].iloc[0]
                    r2 = df_PDQ_Combinaciones['R2'].iloc[0]
                    mse = df_PDQ_Combinaciones['MSE'].iloc[0]
                    rmse = df_PDQ_Combinaciones['RMSE'].iloc[0]
                    error = df_PDQ_Combinaciones['ERROR'].iloc[0]
                    del df_PDQ_Combinaciones

                    # Aplicar el mejor PDQ a la prediccion definitiva
                    model = ARIMA(df_Tmp_ParaPredecir, order=(p, d, q)).fit()
                    forecast = model.forecast(steps=PERIODOS_PREDECIR)

                    predictions = pandas.DataFrame({
                        'PeriodoNum': dates,
                        df_MedIndividualizados[i]: forecast
                    })
                    predictions.set_index('PeriodoNum', inplace=True)

                    print(f'\t{fColors.WARNING}Mejor combinación PDQ: {p}, {d}, {q}.{fColors.ENDC}')
                    print(f'\t{fColors.OKGREEN}R2: {round(r2, 2)}\tMSE: {round(mse, 2)}\tRMSE: {round(rmse, 2)}\tERROR: {round(error, 2)}{fColors.ENDC}')

                    # Esperar solo para alcanzar a ver en pantalla los numeros.
                    #sleep(10)
                    
                    del df_Tmp_ParaPredecir
                    df_Tmp_PrediccionesAcumulado = pandas.concat([df_Tmp_PrediccionesAcumulado, predictions])

                except:
                    # Si la predicción no tiene resultados la omite.
                    pass

            df_Organizado = pandas.concat([df_Organizado, df_Tmp_PrediccionesAcumulado])
            del df_Tmp_PrediccionesAcumulado

            # Almacenar la tabla real+prediccion
            df_Organizado.to_csv(f'{CARPETA_PARAMETROS_CLIENTE}/Real_Prediccion_ARIMA_{CLIENTE}.csv', sep=';', index=True)
            df_PDQ.to_csv(f'{CARPETA_PARAMETROS_CLIENTE}/df_PDQ_ARIMA.csv', sep=';', index=True)

class FuncionesComunes():

    #############################################################################################################################
    # Cargar el excel que lista los laboratorios parametrizados. ################################################################
    #############################################################################################################################

    def fCrearBaseParaProyectar(CLIENTE, CARPETA_DAAS_SISMED, HISTORICO_DESDE_PERIODO, CARPETA_PARAMETROS_CLIENTE, CANAL, ENTIDAD_SISMED, USAR_GRUPO_MEDICAMENTO):

        """
        Las variables de parametros de la funcion significan lo siguiente:
        CLIENTE                     -> Nombre del cliente, que se usará para nombrar los archivos resultantes.
        CARPETA_DAAS_SISMED         -> Carpeta donde esta el DAAS de Sismed (los archivos parquet de la ETL de Qlik).
        HISTORICO_DESDE_PERIODO     -> Periodo desde el cual se tomarán los datos de Sismed DAAS.
        CARPETA_PARAMETROS_CLIENTE  -> Carpeta base donde queda todo lo del cliente que use el producto.
        CANAL                       -> Indicar si es COMERCIAL, INSTITUCIONAL o AMBOS.
        ENTIDAD_SISMED              -> Entidades que vamos a filtrar
        USAR_GRUPO_MEDICAMENTO      -> Cruzar con nombres de medicamento que deben estar al frente del principio activo en _MoleculasCliente.xlsx (Campo debe llamarse GrupoCUMMedicamento)
        """
        print(f'\t{fColors.HEADER}***Generando la base Sismed para proyección.{fColors.ENDC}')

        # Debe existir si o si, los parametros de las moleculas del cliente en un archivo llamado _MoleculasCliente con columa NombrePrincipioActivo
        df_ParametrosMoleculasCliente = pandas.read_excel(f'{CARPETA_PARAMETROS_CLIENTE}/_MoleculasCliente.xlsx', engine='openpyxl', sheet_name="Hoja1", na_filter=True, usecols=["NombrePrincipioActivo"])

        if USAR_GRUPO_MEDICAMENTO == 1:
            df_ParametrosGrupoMedicamentoCliente = pandas.read_excel(f'{CARPETA_PARAMETROS_CLIENTE}/_MoleculasCliente.xlsx', engine='openpyxl', sheet_name="Hoja1", na_filter=True, usecols=["GrupoCUMMedicamento"])

        # Crear dataframe que va a contener la información final de Sismed Procesada
        df_Sismed = pandas.DataFrame()

        for j in listdir(CARPETA_DAAS_SISMED):
            if ".parquet" in j:
                print(f'\t{fColors.OKGREEN}>>>Leyendo archivo {j}{fColors.ENDC}')

                ## Temporalmente se usará el campo de ROL
                if USAR_GRUPO_MEDICAMENTO == 1:
                    df_TmpDataSismed = pandas.read_parquet(f'{CARPETA_DAAS_SISMED}/{j}', engine='pyarrow', columns=['Anio', 'PeriodoNum', 'NombrePrincipioActivo', 'GrupoCUMMedicamento', 'Cantidad', 'TipoReportePrecio', 'RolEntidadReportadora', 'Transaccion', 'Canal', 'EntidadReportadora'] )
                    #df_TmpDataSismed = pandas.read_parquet(f'{CARPETA_DAAS_SISMED}/{j}', engine='pyarrow', columns=['Anio', 'PeriodoNum', 'NombrePrincipioActivo', 'GrupoCUMMedicamento', 'Cantidad', 'TipoReportePrecio', 'TipoEntidadReportadora', 'Transaccion', 'Canal', 'EntidadReportadora'] )
                else:
                    df_TmpDataSismed = pandas.read_parquet(f'{CARPETA_DAAS_SISMED}/{j}', engine='pyarrow', columns=['Anio', 'PeriodoNum', 'NombrePrincipioActivo', 'Cantidad', 'TipoReportePrecio', 'RolEntidadReportadora', 'Transaccion', 'Canal', 'EntidadReportadora'] )
                    #df_TmpDataSismed = pandas.read_parquet(f'{CARPETA_DAAS_SISMED}/{j}', engine='pyarrow', columns=['Anio', 'PeriodoNum', 'NombrePrincipioActivo', 'Cantidad', 'TipoReportePrecio', 'TipoEntidadReportadora', 'Transaccion', 'Canal', 'EntidadReportadora'] )

                # Filtrar solamente Ventas Primarias de Laboratorios
                #### Temporalmente se van a tomar los roles de elabora/importa porque el tipo de entidad no viene en Sismed
                df_TmpDataSismed = df_TmpDataSismed.where((df_TmpDataSismed.PeriodoNum >= HISTORICO_DESDE_PERIODO) & (df_TmpDataSismed.TipoReportePrecio == "VENTA") & (df_TmpDataSismed.RolEntidadReportadora == "ELABORA/IMPORTA") & (df_TmpDataSismed.Transaccion == "PRIMARIA"))
                #df_TmpDataSismed = df_TmpDataSismed.where((df_TmpDataSismed.PeriodoNum >= HISTORICO_DESDE_PERIODO) & (df_TmpDataSismed.TipoReportePrecio == "VENTA") & (df_TmpDataSismed.TipoEntidadReportadora == "LABORATORIO") & (df_TmpDataSismed.Transaccion == "PRIMARIA"))
                df_TmpDataSismed = df_TmpDataSismed.dropna(how = 'all')

                # Filtrar el canal a utilizar, si es ambos o solamente uno
                if CANAL == 'AMBOS':
                    pass
                else:
                    df_TmpDataSismed = df_TmpDataSismed.where(df_TmpDataSismed.Canal == CANAL)
                    df_TmpDataSismed = df_TmpDataSismed.dropna(how = 'all')

                # Filtrar la entidad reportadora (Sismed) a utilizar, si todas o alguna en particular
                if ENTIDAD_SISMED == 'TODAS':
                    pass
                else:
                    df_TmpDataSismed = df_TmpDataSismed.where(df_TmpDataSismed.EntidadReportadora == ENTIDAD_SISMED)
                    df_TmpDataSismed = df_TmpDataSismed.dropna(how = 'all')

                # Usar solamente las dimensiones necesarias para predecir
                df_TmpDataSismed = df_TmpDataSismed.dropna(how = 'all')
                ## Temporalmente no se usa el campo TipoEntidadReportadora
                df_TmpDataSismed = df_TmpDataSismed.drop(['TipoReportePrecio', 'Transaccion', 'Canal'], axis=1)
                #df_TmpDataSismed = df_TmpDataSismed.drop(['TipoReportePrecio', 'TipoEntidadReportadora', 'Transaccion', 'Canal'], axis=1)

                # Limitar los datos a los parametrizados en el Excel _MoleculasCliente.xlsx
                df_TmpDataSismed = df_TmpDataSismed.join(df_ParametrosMoleculasCliente.set_index('NombrePrincipioActivo'), on='NombrePrincipioActivo', how='inner')

                if USAR_GRUPO_MEDICAMENTO == 1:
                    # Limitar los datos a los parametrizados en el Excel _MoleculasCliente.xlsx
                    df_TmpDataSismed = df_TmpDataSismed.join(df_ParametrosGrupoMedicamentoCliente.set_index('GrupoCUMMedicamento'), on='GrupoCUMMedicamento', how='inner')

                # Sumar los multiples valores con group by
                if USAR_GRUPO_MEDICAMENTO == 1:
                    df_TmpDataSismed = df_TmpDataSismed.groupby(by=['Anio', 'PeriodoNum', 'NombrePrincipioActivo', 'GrupoCUMMedicamento'], as_index=False).agg(Cantidad = ("Cantidad", "sum"))
                    df_TmpDataSismed = df_TmpDataSismed.sort_values(by=['NombrePrincipioActivo', 'GrupoCUMMedicamento', 'PeriodoNum'], ascending=True)
                else:
                    df_TmpDataSismed = df_TmpDataSismed.groupby(by=['Anio', 'PeriodoNum', 'NombrePrincipioActivo'], as_index=False).agg(Cantidad = ("Cantidad", "sum"))
                    df_TmpDataSismed = df_TmpDataSismed.sort_values(by=['NombrePrincipioActivo', 'PeriodoNum'], ascending=True)

                df_Sismed = pandas.concat([df_Sismed, df_TmpDataSismed])
                del df_TmpDataSismed

        # Convertir a entero los datos de tiempo
        df_Sismed['Anio'] = df_Sismed['Anio'].astype('int64') 
        df_Sismed['PeriodoNum'] = df_Sismed['PeriodoNum'].astype('int64') 
        
        # Almacenar el dataframe df_Sismed
        print(f'\t{fColors.WARNING}<<<Almacenando archivo {CARPETA_PARAMETROS_CLIENTE}Sismed_Base_{CLIENTE}.csv{fColors.ENDC}')
        df_Sismed.to_csv(f'{CARPETA_PARAMETROS_CLIENTE}/Sismed_Base_{CLIENTE}.csv', sep=';', index=None)
        del df_Sismed    

    #############################################################################################################################
    # Funcion para distribuir los datos predichos, osea que se utiliza si o si el cliente quiere Indicacion #####################
    #############################################################################################################################

    def fUnificarConIndicaciones(CLIENTE, CARPETA_PARAMETROS_CLIENTE):

        print(f'\t{fColors.HEADER}***Creando datos por indicación.{fColors.ENDC}')

        # Crear dataframe que va a contener la información final proyectada y distribuida.
        df_ProyectadoDistribuido = pandas.DataFrame()

        # Leer todas las moleculas previamente proyectadas
        for j in listdir('./Proyecciones/'):
            print(f'\t{fColors.OKGREEN}>>>Leyendo archivo {j}{fColors.ENDC}')
            df_TmpProyectado = pandas.read_csv(f'Proyecciones/{j}', sep=';', decimal=',', encoding="utf-8", low_memory=False, index_col=False, usecols=['Anio', 'Mes', 'NombrePrincipioActivo', 'Cantidad', 'MSE', 'RMSE'], dtype={'Anio': str, 'Mes': str} )

            # Asignar el tipo de prediccion del nombre del archivo
            df_TmpProyectado['Algoritmo'] = j.split('_')[2]

            df_ProyectadoDistribuido = pandas.concat([df_ProyectadoDistribuido, df_TmpProyectado])
            del df_TmpProyectado

        # Crear el campo de PeriodoNum
        df_ProyectadoDistribuido['PeriodoNum'] = df_ProyectadoDistribuido['Anio'] + ("0" + df_ProyectadoDistribuido['Mes']).str[-2:3]

        # Reordenar el dataframe
        df_ProyectadoDistribuido = df_ProyectadoDistribuido[['Anio', 'PeriodoNum', 'NombrePrincipioActivo', 'Cantidad', 'Algoritmo', 'MSE', 'RMSE']]
        
        # Convertir a entero los datos de tiempo
        df_ProyectadoDistribuido['Anio'] = df_ProyectadoDistribuido['Anio'].astype('int64') 
        df_ProyectadoDistribuido['PeriodoNum'] = df_ProyectadoDistribuido['PeriodoNum'].astype('int64') 

        # df_ProyectadoDistribuido.to_csv('Test.csv', sep=';', encoding="utf-8", index=None)

        # Deben existir si o si, los parametros  de las indicaciones anualizado en un archivo llamado _DistribucionIndicaciones y las columnas Key(Anio, NombrePrincipioActivo) Indicacion y Porcentaje
        df_ParametrosDistribucionCliente = pandas.read_excel(f'{CARPETA_PARAMETROS_CLIENTE}/_DistribucionIndicaciones.xlsx', engine='openpyxl', sheet_name="Sheet1", na_filter=True, usecols=['Anio', 'NombrePrincipioActivo', 'Indicacion', 'Porcentaje'])

        # Hacer el join correspondiente para unir los datos base de sismed con las indicaciones anualizada y por principio activo
        df_ProyectadoDistribuido = df_ProyectadoDistribuido.join(df_ParametrosDistribucionCliente.set_index(['Anio', 'NombrePrincipioActivo']), on=['Anio', 'NombrePrincipioActivo'], how='left')

        # Calcular el valor para llevar a la predicción
        df_ProyectadoDistribuido['CantidadDistribuida'] = df_ProyectadoDistribuido['Cantidad'] * df_ProyectadoDistribuido['Porcentaje']

        df_ProyectadoDistribuido.to_csv('Test_Final.csv', sep=';', encoding="utf-8", index=None)

    #############################################################################################################################
    # Mover archivos de la raiz de proyecto (C:\QsGJX\04.Python\ir_latam) a un lugar ############################################
    #############################################################################################################################

    def fMoverArchivosARuta(extension_archivos, ruta_destino):
        vListaArchivos = listdir('./')

        for j in vListaArchivos:
            if f".{extension_archivos}" in j:
                move(j, f"{ruta_destino}/{j}")

        print(f"{fColors.WARNING}\n   * Se mueven los archivos a {ruta_destino} correctamente.{fColors.ENDC}")

    def fCalcularTiempoEjecucionScript(vInicio):
        vTiempoEjecucion = round( (time() - vInicio)/60 , 1)
        print("\nTiempo ejecución del script: " + str(vTiempoEjecucion) + " minutos.\n")
    
    def fCalcularMesesAProyectar(fecha_inicio):
        fecha_final = '2025-12-31'

        F_inicio = datetime.strptime(fecha_inicio, "%Y-%m-%d")
        F_final = datetime.strptime(fecha_final, "%Y-%m-%d")

        return (F_final.year - F_inicio.year) * 12 + F_final.month - F_inicio.month + 1

    def fCrearPeriodosIntermedio(pMin, pMax):

        vListaPeriodos = []
        vPeriodoAListar = int(pMin)

        while vPeriodoAListar <= pMax:

            if vPeriodoAListar%100 <= 12:
                vListaPeriodos.append(vPeriodoAListar)

            if vPeriodoAListar%100 == 13:
                vPeriodoAListar = int(str( int(str(vPeriodoAListar)[:4]) + 1 ) + '01')
            else:
                vPeriodoAListar = int(vPeriodoAListar)+1

        return vListaPeriodos       

    def fAplicarMediaMovilATendencia(dataFrame):

        ## Obtener el periodo mínimo y máximo
        vPeriodoMinimo = dataFrame['PeriodoNum'].min()
        vPeriodoMaximo = dataFrame['PeriodoNum'].max()
        print(f"\t\t\tMínimo > {vPeriodoMinimo}, Máximo > {vPeriodoMaximo}")

        ## Crear los periodos faltantes y hacer Merge para ponerlos en el dataframe original "dataFrame"
        vListaPeriodosAmpliados = FuncionesComunes.fCrearPeriodosIntermedio(vPeriodoMinimo, vPeriodoMaximo)
        df_PeriodosAmpliados = pandas.DataFrame(vListaPeriodosAmpliados, columns=['PeriodoNumNuevos'], index=None)
        dataFrame = dataFrame.merge(df_PeriodosAmpliados, left_on='PeriodoNum', right_on='PeriodoNumNuevos', how='outer')

        ## Procesar para llenar los espacios o suavizar los picos en los datos
        dataFrame['Cantidad'] = dataFrame['Cantidad'].fillna(0)
        dataFrame['Cantidad_MediaMovil'] = dataFrame['Cantidad']

        vListaCantidadesNuevas = []

        for k in range(0, len(dataFrame)):
            try:
                if dataFrame.iloc[k]['Cantidad_MediaMovil'] == 0:

                    # Buscar el numero hacia abajo con valor:
                    vNumeroAbajo = 0
                    vWhile = -1
                    while vNumeroAbajo == 0:
                        vNumeroAbajo = dataFrame.iloc[k+vWhile]['Cantidad_MediaMovil']
                        vWhile = vWhile-1

                        if vWhile == -100:
                            vNumeroAbajo = -99999 #Para detenerlo

                    # Buscar el numero hacia arriba con valor:
                    vNumeroArriba = 0
                    vWhile = 1
                    while vNumeroArriba == 0:
                        vNumeroArriba = dataFrame.iloc[k+vWhile]['Cantidad_MediaMovil']
                        vWhile = vWhile+1

                        if vWhile == 100:
                            vNumeroArriba = 99999 #Para detenerlo

                    vValorMediaMovil = (vNumeroAbajo + vNumeroArriba)/2
                else:
                    vValorMediaMovil = dataFrame.iloc[k]['Cantidad_MediaMovil']
            except:
                vValorMediaMovil = 999999 ## Solo se ponen los 9 para validar en los CSV

            dataFrame.loc[k, 'Cantidad_MediaMovil'] = round(vValorMediaMovil, 2)

        return dataFrame

    def fAplicarOutliersATendencia(dataframe, TIPO_CANTIDAD):

        # Calcular la media y la desviación estándar
        if TIPO_CANTIDAD == 'Original':
            vMedia = dataframe['Cantidad'].mean()
            vMediana = dataframe['Cantidad'].median()
            vDesviacionEstandar = dataframe['Cantidad'].std()
        elif TIPO_CANTIDAD == 'MediaMovil':
            vMedia = dataframe['Cantidad_MediaMovil'].mean()
            vMediana = dataframe['Cantidad_MediaMovil'].median()
            vDesviacionEstandar = dataframe['Cantidad_MediaMovil'].std()

        print(f"\t\t{fColors.HEADER}Media: {vMedia}, Mediana: {vMediana} y Desviación: {vDesviacionEstandar}{fColors.ENDC}")
        vUmbral = 1 #Definición manual

        vAtipicos = dataframe[(dataframe['Cantidad_MediaMovil'] < vMedia - vUmbral * vDesviacionEstandar) | (dataframe['Cantidad_MediaMovil'] > vMedia + vUmbral * vDesviacionEstandar)]
        vAtipicos['_FlagAtipico'] = 1

        vAtipicos = vAtipicos[['PeriodoNum', '_FlagAtipico']]

        dataframe = dataframe.merge(vAtipicos, left_on='PeriodoNum', right_on='PeriodoNum', how='left')
        del vAtipicos

        dataframe['Cantidad_AtipicosCorregidos'] = numpy.where(dataframe['_FlagAtipico'] == 1, vMediana, dataframe['Cantidad_MediaMovil'])
        print(f"Atipicos:\n{dataframe}")

        return dataframe