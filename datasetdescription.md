# Les jeux de données
Les données manipulées sont issues de la suite de test du moteur OLAP de Pentaho Mondrian. Le dataset contient 22 fichiers CSV formattés comme suit:
* Point virgule (";") comme séparateur de champs
* Point (".") comme séparateur décimal
* Les fichiers ne contiennent pas de headers

## Liste des données: 
* **account**: liste des comptes analytiques
* **category**: liste des catégories
* **currency**: liste des monnaies
* **customer**: liste des clients ayant une carte de fidélité
* **days**: liste des jours de semaine
* **department**: liste des départements de l'enseigne
* **employee**: liste des employés 
* **employee_closure**: 
* **expense**: liste les dépenses
* **inventory**: l'inventaire des livraisons entrepôts/magasin
* **position**: liste des grades des employés
* **product**: liste de tous les produits
* **product_class**: liste tous les classes de produit. Chaque produit appartient à une classe 
* **promotion**: liste toutes les promotions faites sur les produits
* **region**: liste les régions où les magasin sont implantés
* **reserve_employee**: liste des employés de réserve
* **salary**: historique des salaires versés aux employés
* **sales**: liste les ventes effectuées<details>
  <summary> Liste des colonnes</summary>
    
    Nom|Type|Commentaires
    --- | --- | ---
    product_id|int
    --- | --- | ---
    time_id|int
    --- | --- | ---
    customer_id|int
    --- | --- | ---
    promotion_id|int
    --- | --- | ---
    store_id|int
    --- | --- | ---
    store_sales|double| Prix de vente au niveau du magasin
    --- | --- | ---
    store_cost|double| Coût de la vente par unité
    --- | --- | ---
    unit_sales|double| Nombre d'unités vendues
    --- | --- | ---
 </details>

* **store**: liste les magasin de l'enseigne<details>
  <summary> Liste des colonnes </summary>

    Nom|Type|Commentaires
    --- | --- | ---
    store_id|int
    --- | --- | ---
    store_type|string
    --- | --- | ---
    region_id|int|
    --- | --- | ---
    store_name|string|
    --- | --- | ---
    store_number|int|
    --- | --- | ---
    store_street_address|string|
    --- | --- | ---
    store_city|string|
    --- | --- | ---
    store_state|string|
    --- | --- | ---
    store_postal_code|int|
    --- | --- | ---
    store_country|string|
    --- | --- | ---
    store_manager|string|
    --- | --- | ---
    store_phone|string|
    --- | --- | ---
    store_fax|string|
    --- | --- | ---
    first_opened_date|date|au format dd/MM/yyyy  hh:mm:ss
    --- | --- | ---
    last_remodel_date|date|au format dd/MM/yyyy  hh:mm:ss
    --- | --- | ---
    store_sqft|int|
    --- | --- | ---
    grocery_sqft|int|
    --- | --- | ---
    frozen_sqft|int|
    --- | --- | ---
    meat_sqft|int|
    --- | --- | ---
    coffee_bar|int|0=false et 1=true
    --- | --- | ---
    video_store|int|0=false et 1=true
    --- | --- | ---
    salad_bar|int|0=false et 1=true
    --- | --- | ---
    prepared_food|int|0=false et 1=true
    --- | --- | ---
    florist|int|0=false et 1=true
    --- | --- | ---  

</details>

* **time_by_day**: liste tous  
* **warehouse**: liste les entrepôts de l'enseigne
* **warehouse_type**: type d'entrepôt
