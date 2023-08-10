## ¿Por qué se hizo esta carpeta?
Se hizo para guardar el archivo Mysql y los archivos resultantes de este.

## ¿Qué contiene esta carpeta?
El archivo ".sql" se creo para poder agregar la tabla que tuviera los id de "distritos" (burough) de Nueva York y luego reemplazar la tabla con los nombres por los id que corresponden a cada uno.<br>
Los .csv exportados desde el archivo .sql con los cambios ya realizados (o un .csv nuevo).<br>
El archivo Prueba.ipynb es la traducción del código en SQL a python para luego aplicarlo en el pipeline de Google Cloud Platform, para realizar luego de las cargas incrementales todos los cambios hechos a cada dataset procesado por dataflow y dejarlos como los finales que terminamos utilizando.<br>
Entonces en este archivo se fueron probando códigos hasta que hacian lo que se suponia y se limpiaban, finalizando dejando solo lo que se va a utilizar (cambiando las rutas de los archivos a donde estan en el Cloud Storage).

## ¿Qué csv´s se editaron?
Los csv afectados por este cambio de id son:<br>
- "Calidad del aire.csv" -> renombrado a "Calidad_aire.csv"
- "Sonido_presencia.csv"
- "taxi-zone.csv" -> también renombrado a "taxi_zone.csv"
La tabla agregada es:
- "Borough" ("Borough.csv")
Cada uno de estos csv´s se guardaron en la carpeta MySQL, su versión anterior sigue en la parte de procesados(en la carpeta que corresponda).