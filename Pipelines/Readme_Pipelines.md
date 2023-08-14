## Pipelines

La automatización del proceso ETL ha sido implementada a través de pipelines desarrollados en Python mediante la utilización de **Apache Beam**, la cual es una herramienta muy usada en Big Data y en **Google Cloud Platform** (GCP), es por esta razón que se utilizó este framework en el desarrollo de este proyecto.

Dentro del archivo "*Pipelines.py*", se encuentran definidas diversas funciones que encapsulan las transformaciones a aplicar sobre los conjuntos de datos almacenados en archivos CSV. Además, se incluye una función denominada "*run*", la cual desempeña el papel crucial en el flujo de datos al ser ejecutada en Dataflow, resultando en la generación de los datos transformados finales.

La ejecución de los pipelines se lleva a cabo mediante la consola, escribiendo lo siguiente:

``/Pipelines.py \``<br>
``    --runner DataflowRunner \``<br>
``    --project moonlit-grail-395212 \``<br>
``    --pipeline [pipeline a usar] ``<br>

Descripción de las opciones de ejecución:
- **--runner**: Es un parámetro obligatorio en el cual se especifica el modo de ejecución. "*DataflowRunner*" indica que se ejecutará en Dataflow, en cambio "*DirectRunner*" establece la ejecución en local.
- **--project**: Es el ID del projecto.
- **--pipeline**: Es un parámetro obligatorio en el que se especifíca cual es el pipeline que se correrá.