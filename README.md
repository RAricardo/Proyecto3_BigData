Video de Sustentaciòn: https://www.youtube.com/watch?v=e7JBPLJWJ-M

Proyecto 3 Big Data Spark

Este proyecto consistió en utilizar Apache Spark para realizar varias consultas sobre un dataset de noticias, en este readme se describe el proceso de diseño e implementación de del análisis. 

# 1. Equipo:

Este proyecto fue realizado individualmente por Ricardo Rafael Azopardo Cadenas, entre el buscador por índice inverso y el algoritmo de Clustering de documentos, se seleccionó el buscador por índice inverso.

# 2. Diseño
 
La metodología de diseño para este proyecto fue **CRISP-DM**. A continuación sus diferentes fases:

## 2.1 Business Understanding

El problema consiste en lograr realizar búsquedas de forma eficiente sobre un dataset de noticias. La búsqueda principal que se debe realizar sobre este dataset consiste en obtener las 5 noticias más relevantes a un determinado término, para ello se deben realizar las siguientes tareas:

-	Hacer ingesta de datos a un ecosistema donde se puedan trabajar, para este se selecciono es file system distribuido de Databricks.
-	Realizar limpieza sobre estos datos, eliminando caracteres especiales, palabras de longitud 1 y stopwords.
-	Construir una tabla de índice inverso, que haga mapeo de términos a sus frecuencias en cada documento.
-	Utilizar esta tabla para indexar directamente los documentos más relevantes sobre este término y obtener los 5 más frecuentes. 

Todas estas tareas se realizaron con Apache Spark y Databricks.
 
## 2.2 Entendimiento de los datos

El dataset utilizado es all the news, el cual contiene 150.000 noticias y pesa aproximadamente 700 MB, este dataset viene dividio en 3 archivos .csv y sus contenidos consisten en noticias de diferentes periódicos. 
El archivo csv tiene las siguientes columnas:

![Captura](https://user-images.githubusercontent.com/40037279/57061203-ad903180-6c81-11e9-835b-5b68682ba460.PNG)

Para este problema, los datos de interés son el id, el título y los contenidos de las noticias.

## 2.3 Preparación de los datos

Como fue descrito anteriormente, el dataset está dividido en 3 archivos .csv con la misma estructura, por lo tanto una vez subido a Databricks, deben ser unidos:

![unions](https://user-images.githubusercontent.com/40037279/57061217-b54fd600-6c81-11e9-96de-802aa9d038de.PNG)

Dataframe resultante:

![df init](https://user-images.githubusercontent.com/40037279/57061226-baad2080-6c81-11e9-90a2-ee874a1dafb6.PNG)

Seguido a esto, se seleccionan las columnas de interes y se tokeniza la columna de contenidos. Esto se logra utilizando el tokenizador de expresiones regulares de Spark.ml.feature, al cual se le indica una expresión regular que elimina caracteres especiales:

![df second](https://user-images.githubusercontent.com/40037279/57061229-bc76e400-6c81-11e9-9371-462d5928c3a6.PNG)

Por último se eliminan los stopwords con una función de Spark, y los datos quedan listos para ser utilizados:

![df third](https://user-images.githubusercontent.com/40037279/57061235-c0a30180-6c81-11e9-914c-58ae0f41e19e.PNG)

## 2.4 Modelado

Para la consulta especifica que trata este problema, se decidió construir una tabla de índice invertido, la cual está conformada filas de cada palabra, con tuplas de sus respectivas frecuencias en cada documento, por lo tanto fue necesario realizar un wordcount por cada documento individualmente, una vez obtenido los wordcounts se organizan los datos en el formato indice inverso:

![index](https://user-images.githubusercontent.com/40037279/57061246-c567b580-6c81-11e9-8e19-dd70b773527e.PNG)

El objetivo de este modelo es utilizarlo para mapear directamente el término a sus frecuencias en cada documento, de tal forma que se puedan realizar búsquedas eficaces para encontrar los términos más relevantes a un documento. 

## 2.5 Evaluación

Para la búsqueda realizada el modelo ha obtenido correctamente los resultados de los 5 documentos más relevantes a un término específico.

Por ejemplo, resultados al término trump:

![trump res](https://user-images.githubusercontent.com/40037279/57061257-cbf62d00-6c81-11e9-9b3b-96b99fdb73bb.PNG)

## 2.6 Despliegue

El modelo se puede ejecutar directamente en el notebook del clúster en databricks, para realizar la búsqueda solo es necesario introducir el término a buscar en el widget para la palabra y la última casilla ejecutara la búsqueda automáticamente (es importante señalar que si el cluster se encuentra terminado, es necesario volver a ejecutar todas las casillas antes de realizar la búsqueda). 

Ejemplo de uso del Widget:

![entrada](https://user-images.githubusercontent.com/40037279/57061261-cf89b400-6c81-11e9-8e8e-ee3cd5db1fff.PNG)
