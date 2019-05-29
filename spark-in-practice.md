# Spark in practice
Quoi de mieux que des exemples issus de la pratique pour apprendre à développer avec Spark (Core, DataFrame, DataSet, SQL) ? Rien de mieux. Pour cela, nous allons aborder des uses cases issus de la grande distribution.  

# Les jeux de données
La description des données utilisées pour ses exercices est accessible [ici](https://github.com/Ahmed-Gater/spark-in-practice/blob/master/datasetdescription.md).
# Spark Core

<details><summary>Exercice 1: Quel est le point d'entrée pour un Job Spark ?</summary>
<p>

#### C'est SparkSession !!!
```
import org.apache.spark.sql.SparkSession;
SparkSession sparkSession = SparkSession
                .builder()
                .appName("Mining Frequent Itemset/Assiocation rules from purchasing basket")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "warehouseLocation") //adding config parameters
                .getOrCreate();
```
</p>
</details>

<details><summary>Exercice 2: Charger le fichier des ventes (sales.csv) dans une RDD de type String

```
JavaRDD<String> salesAsStringRDD = ...
```

</summary>  
<p>

```
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
JavaRDD<String> salesAsStringRDD = jsc.textFile("data/sales.csv");
// Afficher 4 éléments de la RDD
salesAsStringRDD.take(4).stream().forEach(System.out::println);
```
avec comme résultat:

```
product_id;time_id;customer_id;promotion_id;store_id;store_sales;store_cost;unit_sales
337;371;6280;0;2;1.5;0.51;2.0
1512;371;6280;0;2;1.62;0.6318;3.0
963;371;4018;0;2;2.4;0.72;1.0

```
</p>
</details>

<details><summary>Exercice 3: Charger le fichier des ventes (sales.csv) dans une RDD de type Sale. La classe Sale est aussi à développer

```
JavaRDD<Sale> salesAsSaleObject = ...
```
</summary>  

```
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

JavaRDD<Sale> salesAsObjects = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile("data/sales.csv")
                .map((Function<String, Sale>) s -> Sale.parse(s, ";"));
```
avec comme résultat:

```
Sale(productId=337, timeId=371, customerId=6280, promotionId=0, storeId=2, storeSales=1.5, storeCost=0.51, unitSales=2.0)
Sale(productId=1512, timeId=371, customerId=6280, promotionId=0, storeId=2, storeSales=1.62, storeCost=0.6318, unitSales=3.0)
Sale(productId=963, timeId=371, customerId=4018, promotionId=0, storeId=2, storeSales=2.4, storeCost=0.72, unitSales=1.0)
```
</details>

<details><summary>Exercice 4: Calculer le chiffre d'affaire par magasin

```
JavaPairRDD<Integer, Double> storeCA = ...
avec un résultat correspondant à: 
Magasin : 23 a un chiffre d'affaires : 151039.54000000007
Magasin : 17 a un chiffre d'affaires : 502334.1299999994
Magasin : 8 a un chiffre d'affaires : 265264.4699999993
Magasin : 11 a un chiffre d'affaires : 364652.1300000001
Magasin : 20 a un chiffre d'affaires : 68089.59
...
```
</summary>  

* Solution avec reduceByKey (à privilègier)

```
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

JavaPairRDD<Integer, Double> storeCA = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(filePath)
                .map((Function<String, Sale>) s -> Sale.parse(s, ";"));
                .mapToPair((PairFunction<Sale, Integer, Double>) sale -> new Tuple2<>(sale.getStoreId(), sale.getStoreSales() * sale.getUnitSales()))
                .reduceByKey((Function2<Double, Double, Double>) (a, b) -> a + b);
storeCA.collectAsMap().forEach((k,v) -> System.out.println("Magasin : " + k + " a un chiffre d'affaires : " + v));
```

* Solution avec reduceByKey (à privilègier)

```
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

JavaPairRDD<Integer, Double> storeCA = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(filePath)
                .map((Function<String, Sale>) s -> Sale.parse(s, ";"))
                .mapToPair((PairFunction<Sale, Integer, Double>) sale -> new Tuple2<>(sale.getStoreId(), sale.getStoreSales() * sale.getUnitSales()))
                .groupByKey()
                .mapToPair((PairFunction<Tuple2<Integer, Iterable<Double>>, Integer, Double>) storeSalesCA -> new Tuple2<>(storeSalesCA._1(),
                        StreamSupport.stream(storeSalesCA._2().spliterator(), false).reduce((x, y) -> x + y).get())
                );
        storeCA.collectAsMap().forEach((k,v) -> System.out.println("Magasin : " + k + " a un chiffre d'affaires : " + v));
  
```

</details>

<details><summary>Exercice 5: Calculer le nombre d'unités vendues par magasin

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

```
Map<Integer, Long> numberUnitsByStore = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile(filePath)
                .map((Function<String, Sale>) s -> Sale.parse(s, ";"))
                .mapToPair((PairFunction<Sale, Integer, Double>) sale -> new Tuple2<>(sale.getStoreId(), sale.getUnitSales()))
                .countByKey();
numberUnitsByStore.forEach((k,v) -> System.out.println("Magasin : " + k + " a un vendu : " + v + " unités"));
```

</details>

<details><summary>Exercice 6: Calculer le chiffre d'affaire par région.  

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

```
// Lecture du fichier store à broadcaster (fichier très petit)
Map<Integer, Integer> storeRegionMapRdd = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile("data/store.csv")
                .mapToPair((PairFunction<String, Integer, Integer>) s -> {
                    Store parse = Store.parse(s);
                    return new Tuple2<>(parse.getId(), parse.getRegionId());
                })
                .collectAsMap();
Broadcast<Map<Integer, Integer>> storeRegionMap = sparkSession.sparkContext().broadcast(storeRegionMapRdd, ClassTag$.MODULE$.apply(Map.class));

// Faire un Map-side Join
JavaPairRDD<Integer, Double> caByRegion = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())
                .textFile("data/sales.csv")
                .mapToPair((PairFunction<String, Integer, Double>) s -> {
                    Sale sale = Sale.parse(s);
                    return new Tuple2<>(storeRegionMap.value().getOrDefault(sale.getStoreId(), -1),
                            sale.getUnitSales() * sale.getStoreSales());
                })
                .reduceByKey((Function2<Double, Double, Double>) (a, b) -> a + b);

caByRegion.collectAsMap().forEach((k,v) -> System.out.println("Region : " + k + " avec un CA : " + v ));
```

</details>

# Spark DataFrame

# Spark DataSet

# Spark SQL

