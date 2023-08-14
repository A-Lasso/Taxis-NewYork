#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import

import argparse
import logging
import pandas as pd
import numpy as np
import re

import apache_beam as beam
from apache_beam.dataframe import expressions
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.io import read_csv
from apache_beam.options.pipeline_options import PipelineOptions

Path = 'gs://tablas_no_procesadas/taxi+_zone_lookup.csv'

def taxizone_pipeline(pipeline):
    # Comienza el pipeline
    with pipeline as p:
        df_taxizone = p | read_csv('gs://tablas_no_procesadas/taxi+_zone_lookup.csv')
        # Procesamiento en pandas
        filas_del = [56, 103, 104, 263, 264]
        with expressions.allow_non_parallel_operations():
            df_taxizone = df_taxizone.drop(filas_del, axis = 0)
        df_taxizone.to_csv('taxi-zone.csv', index = False)
        # Termina la función del pipeline

def calidadDelAire_pipeline(pipeline):
    # Comienza pipeline
    with pipeline as p:
        df_calidadDelAire = p | read_csv('gs://tablas_no_procesadas/Air_Quality.csv')
        # Procesamiento en pandas
        def extract_season(year_str):
            match = re.search(r'(Summer|Winter)', year_str)
            if match:
                return match.group(0)
            else:
                return 'Annual'
        def extract_year(period):
            match = re.search(r'\d{2}$', period)
            if match:
                return f'20{match.group()}'
            else:
                return None
        with expressions.allow_non_parallel_operations():
            df_calidadDelAire['Stations'] = df_calidadDelAire['Time Period'].apply(extract_season)
            df_calidadDelAire['Final Date'] = df_calidadDelAire['Time Period'].apply(extract_year)
            df_calidadDelAire.drop(columns=['Time Period'], inplace=True)
            df_calidadDelAire.drop(columns=['Message', 'Start_Date', 'Unique ID', 'Indicator ID', 'Geo Join ID'], inplace=True)
            df_calidadDelAire = df_calidadDelAire[df_calidadDelAire['Geo Type Name'].isin(['Borough', 'Citywide'])]
            df_calidadDelAire = df_calidadDelAire.sort_values(by='Name', ascending=True)
        df_calidadDelAire.to_csv("gs://tablas_preprocesadas/Calidad del aire.csv", index = False)
        # Termina la función del pipeline

def contaminacion_sonora_pipeline(pipeline):
    # Comienza el pipeline
    with pipeline as p:
        df_contaminacion_sonora = p | read_csv('gs://tablas_no_procesadas/sonidos_NY.csv')
        # Procesamiento en pandas
        df_contaminacion_sonora = df_contaminacion_sonora[['split','sensor_id','borough','year','5-1_car-horn_presence','5-2_car-alarm_presence',
                                                        '5-4_reverse-beeper_presence','1_engine_presence','2_machinery-impact_presence',
                                                        '3_non-machinery-impact_presence','4_powered-saw_presence','5_alert-signal_presence',
                                                        '6_music_presence','7_human-voice_presence','8_dog_presence']]
        borough_mapping = {1: 'Manhattan',
                           3: 'Brooklyn',
                           4: 'Queens'    }
        df_contaminacion_sonora['borough'] = df_contaminacion_sonora['borough'].replace(borough_mapping)
        df_contaminacion_sonora.to_csv("gs://tablas_preprocesadas/Sonido_presencia.csv", index = False)
        # Termina la función del pipeline

def energia_pipeline(pipeline):
    # Comienza el pipeline
    with pipeline as p:
        df_energia = p | read_csv('gs://tablas_no_procesadas/energy.csv')
        # Procesamiento en pandas
        df_energia = df_energia.loc[(df_energia['Country'] == 'United States') & (df_energia['Year'] >= 2000)]
        df_energia.drop(columns='Unnamed: 0', inplace=True)
        # with expressions.allow_non_parallel_operations():
        #     df_taxizone = df_taxizone.drop(filas_del, axis = 0)
        df_energia | 'Escritura en BigQuery' >> beam.io.WriteToBigQuery(
            table='moonlit-grail-395212.Taxis_Automatizado.Energia',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,  # Opciones: WRITE_APPEND, WRITE_EMPTY
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # Termina la función del pipeline

def Id_y_CambiosTablas_pipeline(pipeline):
    # Comienza el pipeline
    taxi_zone = pd.read_csv('gs://tablas_no_procesadas/taxi-zone.csv')
    Sonido_presencia=pd.read_csv('gs://tablas_no_procesadas/Sonido_presencia.csv')
    Calidad_aire=pd.read_csv('gs://tablas_no_procesadas/Calidad del aire.csv')
    Borough={"id_Borough":list(range(0,len(taxi_zone.Borough.unique()))),
             "Borough":list(taxi_zone.Borough.unique())}
    Borough=pd.DataFrame(Borough)
    # Verificar que se encuentren todos los nombres de lugares del resto de dataframes
    for i in list(Calidad_aire["Geo Place Name"].unique()):
        if i not in list(Borough["Borough"]):
            Dicc={"id_Borough":Borough["id_Borough"].max()+1,
                "Borough":i}
            df=pd.concat([Borough,pd.DataFrame([Dicc])],ignore_index=True)
    for i in list(Sonido_presencia["borough"].unique()):
        if i not in list(Borough["Borough"]):
            Dicc={"id_Borough":Borough["id_Borough"].max()+1,
                "Borough":i}
            Borough=pd.concat([Borough,pd.DataFrame([Dicc])],ignore_index=True)
    #Borough.to_csv("ruta de BigQuery",index=False)
    Borough | 'Escritura en BigQuery' >> beam.io.WriteToBigQuery(
        table='moonlit-grail-395212.Taxis_Automatizado.Borough',
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,  # Opciones: WRITE_APPEND, WRITE_EMPTY
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )    

    ## Sonido_presencia:
    # Enlisto las columnas del csv para luego reemplazar la que se llamara borough.
    columnas=list(Sonido_presencia.columns)
    columnas_reemplazada=[columna.replace("borough","Borough") for columna in columnas]
    cambio=pd.DataFrame()
    # Hago el df temporal que va a tener las columnas con el renombre.

    cambio[columnas_reemplazada]=Sonido_presencia[columnas].copy()
    Prueba_sonido_presencia=pd.merge(cambio,Borough,on="Borough",how='inner')

    # Elimino variables ya no usadas (ahorro de memoria/espacio)
    del(cambio)
    del(columnas_reemplazada)

    # Me aseguro de no escribir el nombre directamente si a futuro nos gustaria cambiarlo.
    # Si hay que asegurarse que el id quede en la primera columna del df.
    id=list(Borough.columns)[0]

    # Me guardo las columnas menos "Borough" ya que quiero generar un df solo con los id.
    columnas=list(Prueba_sonido_presencia.columns.drop("Borough"))

    # Reordeno las columnas, ya sé que la columna de id´s queda a lo último y por eso directamente hago pop()
    columnas.pop()

    # Inserto el nombre de la columna de los id(esto para que quede como 4ta columna siempre)
    columnas.insert(3,id)
    Sonido_presencia2=Prueba_sonido_presencia[columnas].copy()

    # Ya no se usan, se elimina(ahorro).
    del(Prueba_sonido_presencia)
    del(Sonido_presencia)
    del(columnas)

    #Exportar a tabla de BigQuery
    Sonido_presencia2 | 'Escritura en BigQuery' >> beam.io.WriteToBigQuery(
        table='moonlit-grail-395212.Taxis_Automatizado.Sonido_presencia',
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,  # Opciones: WRITE_APPEND, WRITE_EMPTY
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    ## taxi-zone.csv:
    # Se crea el df que va a contener los datos temporalmente
    Prueba_taxi_zone=pd.merge(taxi_zone,Borough,on="Borough",how='inner')
    # Se quita la columna con los nombres.
    Prueba_taxi_zone.drop(columns="Borough",inplace=True)
    # Se enlista las columnas del df para hacer el cambio de orden.
    columns=list(Prueba_taxi_zone.columns)
    # Se quita el nombre de la ultima columna (la que contiene los id´s)
    columns.pop()
    # Ahora se inserta este mismo para ser la columna 2.
    columns.insert(1,id)
    # Y ahora renombramos a "taxi_zone" como el resultado de reordenar las columnas.
    taxi_zone=Prueba_taxi_zone[columns].copy()
    #taxi-zone terminado.
    del(columns)
    del(Prueba_taxi_zone)
    #taxi_zone.to_csv("ruta de BigQuery, renombrar a taxi_zone.csv",index=False)
    taxi_zone | 'Escritura en BigQuery' >> beam.io.WriteToBigQuery(
        table='moonlit-grail-395212.Taxis_Automatizado.taxi_zone',
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,  # Opciones: WRITE_APPEND, WRITE_EMPTY
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    ## Calidad del aire.csv
    # Creo el df temporal que contiene el merge.
    Prueba_Calidad_aire=pd.merge(Calidad_aire,Borough,left_on='Geo Place Name', right_on='Borough',how='inner')
    # Guardo las columnas eliminando las que no necesito en la nueva tabla
    columns=list(Prueba_Calidad_aire.columns.drop(['Geo Place Name','Borough',id]))
    # La columna con id´s si la necesito pero la elimine para insertarla en un buen lugar.
    columns.insert(4,id)
    # Ya genero el df final con las columnas que necesito en el orden que necesito.
    # y renombro la columna "Final Date" por "Year" para que coincida con otros csv´s.
    Final_aire=Prueba_Calidad_aire[columns].copy()
    del(columns)
    del(Prueba_Calidad_aire)
    Final_aire["Year"]=Final_aire["Final Date"]
    Final_aire.drop(columns="Final Date",inplace=True)
    #Final_aire.to_csv("ruta de BigQuery, renombrar a Calidad_aire",index=False)
    Final_aire | 'Escritura en BigQuery' >> beam.io.WriteToBigQuery(
        table='moonlit-grail-395212.Taxis_Automatizado.Calidad_aire',
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,  # Opciones: WRITE_APPEND, WRITE_EMPTY
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )    
    # Termina la función del pipeline


def electric_vehicles_pipeline(pipeline):
    # Comienza el pipeline
    with pipeline as p:
        df_ev = p | read_csv('gs://tablas_no_procesadas/ev_per_county_ny_raw.csv')
        # Procesamiento en pandas
        with expressions.allow_non_parallel_operations():
            df_ev['County'] = df_ev['County'].replace(['New York County', 'Queens County', 'Kings County', 'Richmond County', 'Bronx County'], ['Manhattan', 'Queens', 'Brooklyn', 'Staten Island', 'Bronx'])
            df_ev.replace(np.nan, 0, inplace=True)
            df_ev['LD EV Market Share'] = df_ev['LD EV Market Share'].str.replace('%', '')
            df_ev['LD EV Market Share'] = df_ev['LD EV Market Share'].str.replace(',', '.')
            df_ev['LD EV Market Share'] = df_ev['LD EV Market Share'].astype(float)
            df_ev.rename(columns={'LD EV Market Share': 'LD EV Market Share (%)'}, inplace=True)

            df_ev['LDVs per 1k People'] = df_ev['LDVs per 1k People'].str.replace(',', '.')
            df_ev['LDVs per 1k People'] = df_ev['LDVs per 1k People'].astype(float)

            df_ev['BEVs per 1k People'] = df_ev['BEVs per 1k People'].str.replace(',', '.')
            df_ev['BEVs per 1k People'] = df_ev['BEVs per 1k People'].astype(float)

            df_ev['PHEVs per 1k People'] = df_ev['PHEVs per 1k People'].str.replace(',', '.')
            df_ev['PHEVs per 1k People'] = df_ev['PHEVs per 1k People'].astype(float)

            df_ev['EVs per Level 2 Port'] = df_ev['EVs per Level 2 Port'].str.replace(',', '.')
            df_ev['EVs per Level 2 Port'] = df_ev['EVs per Level 2 Port'].astype(float)

            df_ev.drop([63,64], axis=0, inplace=True)
            df_ev | 'Escritura en BigQuery' >> beam.io.WriteToBigQuery(
                table='moonlit-grail-395212.Taxis_Automatizado.ev_per_county_ny',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,  # Opciones: WRITE_APPEND, WRITE_EMPTY
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
    # Termina la función del pipeline

def run(argv = None):
    # Punto de entrada principal, argumentos de ejecución para el pipeline
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--runner',
        required=True,
        help='Runner que se usará para ejecutar el pipeline')
    parser.add_argument(
        '--pipeline',
        dest='pipeline',
        default='taxizone_pipeline',
        help="Elección del pipeline. Debe ser: funcion_pipeline, etc..")
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline = beam.Pipeline(options=PipelineOptions(pipeline_args))
    
    if known_args.pipeline == 'taxizone_pipeline':
        taxizone_pipeline(pipeline)
    elif known_args.pipeline == 'calidadDelAire_pipeline':
        calidadDelAire_pipeline(pipeline)
    elif known_args.pipeline == 'contaminacion_sonora_pipeline':
        contaminacion_sonora_pipeline(pipeline)
    elif known_args.pipeline == 'energia_pipeline':
        energia_pipeline(pipeline)
    elif known_args.pipeline == 'Id_y_CambiosTablas_pipeline':
        Id_y_CambiosTablas_pipeline(pipeline)
    elif known_args.pipeline == 'electric_vehicles_pipeline':
        electric_vehicles_pipeline(pipeline)
    else:
        raise ValueError(f'No se reconoce el valor de --pipeline: {known_args.pipeline!r}.')

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()