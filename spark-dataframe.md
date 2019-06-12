# Spark DataFrame

## Introduction
La description des données utilisées pour ses exercices est accessible [ici](https://github.com/Ahmed-Gater/spark-in-practice/blob/master/datasetdescription.md).
La version de Spark pour ces exercices est 2.4.2 et scala 11.  

## Exercices

<details><summary>Exercice 1: Réécrire les exercices de la section SparkCore avec l'API DataFrame ?</summary>
<p>

<details><summary>Solution de l'exercice 2: Charger le fichier des ventes (sales.csv) dans un DataFrame

```
DataSet<Row> salesAsDF = ...
```

</summary>  
<p>
  
#### Un fichier peut être chargé en laissant l'API inférrer le schéma ou définir son propre schéma avec DataTypes !!!
```
// Version 1: Charger sans schéma
Dataset<Row> salesAsDFWithoutSchemaInferring = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .load(filePath);

// Version 2: Charger le fichier en définissant un schéma
ArrayList<StructField> fields = new ArrayList<>(
                Arrays.asList(
                        DataTypes.createStructField("PRODUCT_ID", DataTypes.LongType, true),
                        DataTypes.createStructField("TIME_ID", DataTypes.IntegerType, true),
                        DataTypes.createStructField("CUSTOMER_ID", DataTypes.LongType, true),
                        DataTypes.createStructField("PROMOTION_ID", DataTypes.LongType, true),
                        DataTypes.createStructField("STORE_ID", DataTypes.IntegerType, true),
                        DataTypes.createStructField("STORE_SALES", DataTypes.DoubleType, true),
                        DataTypes.createStructField("STORE_COST", DataTypes.DoubleType, true),
                        DataTypes.createStructField("UNIT_SALES", DataTypes.DoubleType, true)
                ));
StructType schema = DataTypes.createStructType(fields);
Dataset<Row> salesAsDFWithSchemaDefined = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(schema)
                .load(filePath);
```

avec les schémas correspondant:

```
// Printing schema of dataframe loaded by inferring schema
salesAsDFWithoutSchemaInferring.printSchema() ;
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: string (nullable = true)
 |-- _c5: string (nullable = true)
 |-- _c6: string (nullable = true)
 |-- _c7: string (nullable = true)


// Printing schema of dataframe loaded by defining schema
salesAsDFWithSchemaDefined.printSchema() ;
root
 |-- PRODUCT_ID: long (nullable = true)
 |-- TIME_ID: integer (nullable = true)
 |-- CUSTOMER_ID: long (nullable = true)
 |-- PROMOTION_ID: long (nullable = true)
 |-- STORE_ID: integer (nullable = true)
 |-- STORE_SALES: double (nullable = true)
 |-- STORE_COST: double (nullable = true)
 |-- UNIT_SALES: double (nullable = true)
```
</p>
</details>

<details><summary>Solution de l'exercice 3: charger le fichier des ventes (sales.csv) dans une Dataset<Sale>

```
Dataset<Sale> as  = ...
```
</summary>  

#### On peut transformer un Dataset\<Row> à un Dataset\<Sale> en utilisant un encoder ou un aprés mapping du dataframe (si on veut dériver des objets avant d'appliquer l'encoder) !!!

```
// Version 1: transformer avec un encoder
ArrayList<StructField> fields = new ArrayList<>(
                Arrays.asList(
                        DataTypes.createStructField("productId", DataTypes.LongType, true),
                        DataTypes.createStructField("timeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("customerId", DataTypes.LongType, true),
                        DataTypes.createStructField("promotionId", DataTypes.LongType, true),
                        DataTypes.createStructField("storeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("storeSales", DataTypes.DoubleType, true),
                        DataTypes.createStructField("storeCost", DataTypes.DoubleType, true),
                        DataTypes.createStructField("unitSales", DataTypes.DoubleType, true)
                ));
StructType schema = DataTypes.createStructType(fields);
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(schema)
                .load(filePath);
Encoder<Sale> saleEncoder = Encoders.bean(Sale.class);
Dataset<Sale> as = salesAsDF.as(saleEncoder);
as.printSchema();

// Version 2: transformer avec une Map
ArrayList<StructField> fields = new ArrayList<>(
                Arrays.asList(
                        DataTypes.createStructField("productId", DataTypes.LongType, true),
                        DataTypes.createStructField("timeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("customerId", DataTypes.LongType, true),
                        DataTypes.createStructField("promotionId", DataTypes.LongType, true),
                        DataTypes.createStructField("storeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("storeSales", DataTypes.DoubleType, true),
                        DataTypes.createStructField("storeCost", DataTypes.DoubleType, true),
                        DataTypes.createStructField("unitSales", DataTypes.DoubleType, true)
                ));
StructType schema = DataTypes.createStructType(fields);
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(schema)
                .load(filePath);

Dataset<Sale> salesAsDataSet = salesAsDF.map((MapFunction<Row, Sale>) row -> Sale
                .builder()
                .productId((Integer) row.get(0))
                .timeId((Integer) row.get(1))
                .customerId((Long) row.get(2))
                .promotionId((Integer) row.get(3))
                .storeId((Integer) row.get(4))
                .storeSales((Double) row.get(5))
                .storeCost((Double) row.get(6))
                .unitSales((Double) row.get(7))
                .build(), 
                Encoders.bean(Sale.class));
salesAsDataSet.printSchema();
```
Et les schémas du DataSet est:

```
// Schema avec la version 1
root
 |-- productId: long (nullable = true)
 |-- timeId: integer (nullable = true)
 |-- customerId: long (nullable = true)
 |-- promotionId: long (nullable = true)
 |-- storeId: integer (nullable = true)
 |-- storeSales: double (nullable = true)
 |-- storeCost: double (nullable = true)
 |-- unitSales: double (nullable = true)

// Schema avec la version 2
root
 |-- customerId: long (nullable = true)
 |-- productId: integer (nullable = true)
 |-- promotionId: integer (nullable = true)
 |-- storeCost: double (nullable = true)
 |-- storeId: integer (nullable = true)
 |-- storeSales: double (nullable = true)
 |-- timeId: integer (nullable = true)
 |-- unitSales: double (nullable = true)


```
</details>

<details><summary>Solution de l'exercice 4 avec DataFrame: Calculer le chiffre d'affaire par magasin

```
Le résultat peut correspondre à: 
+-------+------------------+
|storeId|        sum(rowCA)|
+-------+------------------+
|     12| 265012.0099999998|
|     22|18206.400000000005|
|      1|164537.21000000037|
|     13| 537768.1800000002|
|      6| 310913.3200000007|
...
```
</summary>  

#### On peut le faire avec la méthode groupBy et sum ou bien avec la fonction agg !!!

* Solution avec groupBy et sum

```
ArrayList<StructField> fields = new ArrayList<>(
                Arrays.asList(
                        DataTypes.createStructField("productId", DataTypes.LongType, true),
                        DataTypes.createStructField("timeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("customerId", DataTypes.LongType, true),
                        DataTypes.createStructField("promotionId", DataTypes.LongType, true),
                        DataTypes.createStructField("storeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("storeSales", DataTypes.DoubleType, true),
                        DataTypes.createStructField("storeCost", DataTypes.DoubleType, true),
                        DataTypes.createStructField("unitSales", DataTypes.DoubleType, true)
                ));
StructType schema = DataTypes.createStructType(fields);
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(schema)
                .load(filePath);
Dataset<Row> caByStore = salesAsDF.select(col("storeId"), col("storeSales").multiply(col("unitSales")).as("rowCA"))
                .groupBy(col("storeId"))
                .sum("rowCA").as("ca");
caByStore.show();
```

* Solution avec groupBy et agg (plusieurs fonctions d'aggrégation sont implémentées)
```
Dataset<Row> caByStore = salesAsDF.select(col("storeId"), col("storeSales").multiply(col("unitSales")).as("rowCA"))
                .groupBy(col("storeId"))
                .agg(sum(col("rowCA")));
```

Le résultat ressemble à:
```
+-------+------------------+
|storeId|        sum(rowCA)|
+-------+------------------+
|     12| 265012.0099999998|
|     22|18206.400000000005|
|      1|164537.21000000037|
|     13| 537768.1800000002|
|      6| 310913.3200000007|
```

</details>

<details><summary>Solution de l'exercice 5 avec DataFrame: Calculer le nombre d'unités vendues par magasin

```
Map<Integer, Long> numberUnitsByStore = ...
avec un résultat correspondant à: 
Magasin : 5 a un vendu : 1298 unités
Magasin : 10 a un vendu : 7898 unités
Magasin : 24 a un vendu : 15732 unités
Magasin : 14 a un vendu : 2593 unités
...
```
</summary>  

#### Utiliser la fonction agg présentée dans l'exercice 4 
```
ArrayList<StructField> fields = new ArrayList<>(
                Arrays.asList(
                        DataTypes.createStructField("productId", DataTypes.LongType, true),
                        DataTypes.createStructField("timeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("customerId", DataTypes.LongType, true),
                        DataTypes.createStructField("promotionId", DataTypes.LongType, true),
                        DataTypes.createStructField("storeId", DataTypes.IntegerType, true),
                        DataTypes.createStructField("storeSales", DataTypes.DoubleType, true),
                        DataTypes.createStructField("storeCost", DataTypes.DoubleType, true),
                        DataTypes.createStructField("unitSales", DataTypes.DoubleType, true)
                ));
StructType schema = DataTypes.createStructType(fields);
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(schema)
                .load(filePath);
Dataset<Row> agg = salesAsDF.select(col("storeId"), col("unitSales"))
                .groupBy(col("storeId"))
                .agg(count("unitSales"));
List<Row> rows = agg.collectAsList();
Map<Integer, Long>  storeUnitSales = new HashedMap() ;
rows.stream().forEach(s -> storeUnitSales.put(s.getInt(0),s.getLong(1)));
```

</details>

<details><summary>Solution de l'exercice 6 avec DataFrame: Calculer le chiffre d'affaire par région.  

```
JavaPairRDD<Integer, Double> caByRegion = ...
avec un résultat correspondant à: 
Region : 23 avec un CA : 537768.1800000002
Region : 89 avec un CA : 151039.54000000007
Region : 26 avec un CA : 265264.4699999993
Region : 47 avec un CA : 310913.3200000007
Region : 2 avec un CA : 76719.89
...
```
</summary>

#### Le moteur sql trouvera lui-même quel schéma de jointure le mieux adapté aux datasets ou le forcer à broadcaster le store.csv dataset !!!

```
// Lecture du fichier store à broadcaster (fichier très petit)
Dataset<Row> storesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Store.SCHEMA)
                .load(storeFilePath)
                .select(col("id"),col("regionId")) ;
// Charger le fichier sales.csv
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath);
// Sans broadcast
Dataset<Row> caByRegion = salesAsDF.join(storesAsDF, salesAsDF.col("storeId").equalTo(storesAsDF.col("id")))
                .groupBy(col("storeId"))
                .agg(sum(col("storeSales").multiply(col("unitSales"))).as("regionCA"));

// En forçant le broadcast
Dataset<Row> caByRegionWithBroadcast = salesAsDF.join(broadcast(storesAsDF), salesAsDF.col("storeId").equalTo(storesAsDF.col("id")))
                .groupBy(col("storeId"))
                .agg(sum(col("storeSales").multiply(col("unitSales"))).as("regionCA"));
```

</details>

<details><summary>Solution de l'exercice 7 avec DataFrame: Comparer les ventes (en termes de CA) entre les premiers trimestres (Q1) de 1997 et 1998    

```
JavaPairRDD<Integer, Double>  yearCAQuarter= ...

CA Q1 de l'année 1997 : 460615.02999999735
CA Q1 de l'année 1998 : 965701.8800000021
...
```

</summary>

```
// Lecture du fichier store à broadcaster (fichier très petit)
Dataset<Row> times = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(TimeByDay.SCHEMA)
                .load(timeByDayFilePath)
                .where(col("quarter").equalTo(quarter))
                .select(col("timeId"),col("theYear")) ;


// Charger le fichier sales.csv
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath);

Dataset<Row> caByQuarter = salesAsDF.join(broadcast(times), salesAsDF.col("timeId").equalTo(times.col("timeId")), "right_outer");
caByQuarter.show();

```

</details>

<details><summary>Solution de l'exercice 8 avec DataFrame:       

```
Création du client ES: ESClient es = new ESClient("localhost",9200)  
Envoi de documents vers ES: es.index("sales",map);
...
```

</summary>
La fonction util.rowToMap transforme une Row à une Map indexable sur Elasticsearch

```
// Charger le fichier sales.csv
Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath);

salesAsDF.foreachPartition(new ForeachPartitionFunction<Row>() {
            ESClient es = new ESClient("localhost",9200) ;
            @Override
            public void call(Iterator<Row> iterator) throws Exception {
                iterator.forEachRemaining(sale -> {
                    es.index("sales",Util.rowToMap(sale));
                });
            }
        });
```

</details>


<details><summary>Solution de l'exercice 9 avec DataFrame: calculer le chiffre d'affaires généré par niveau d'instruction (colonne education du fichier customer.csv.
  Petite contrainte, le fichier customer.csv ne peut pas être broadcasté en l'état.
  Le résultat attendu est:
  
  ```
  Education level : Graduate Degree a un chiffre d'affaires : 284358.7000000002
  Education level : High School Degree a un chiffre d'affaires : 1614680.6999999923
  Education level : Partial College a un chiffre d'affaires : 506574.38000000064
  Education level : Bachelors Degree a un chiffre d'affaires : 1394302.7699999944
  Education level : Partial High School a un chiffre d'affaires : 1650653.7099999934
  ```
  
  </summary>
  L'idée de cette question est quand on fait une jointure, on réduit au maximum les données sur lesquelles on ne garde que les donnée nécessaires à la jointure pour réduire le coût du shuffling. 
  
  ```
  // Lecture du fichier customer à broadcaster
  Dataset<Row> customerEducation = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Customer.SCHEMA)
                .load(customerFilePath)
                .select(col("customerId").as("cid"),col("education"))
                ;

  // Charger le fichier sales.csv
  Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath)
                .select(col("customerId"),col("storeSales").multiply(col("unitSales")).as("rowCA"))
                ;
  Dataset<Row> sum = salesAsDF.join(customerEducation, salesAsDF.col("customerId").equalTo(customerEducation.col("cId")))
                .groupBy(col("education"))
                .agg(sum(col("rowCA")).as("caByEducationLevel"))
                ;
  ```
     
  </details>


<details><summary>Solution de l'exercice 10 avec DataFrame: similaire à l'exercice 8 mais en stockant les résultats sous format CSV sur HDFS.
  </summary>
  
 
  ```
 Dataset<Row> salesAsDF = sparkSession
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", "false")
                .schema(Sale.SCHEMA)
                .load(salesFilePath) ;
 // Stocker le DF au format CSV
 salesAsDF.write()
          .format("csv")
          .mode(SaveMode.Ignore)
          .save(destinationFilePath);
  ```
  
  </details>
