# Calculations For numeric-validations.edn

```sql
CREATE TABLE trips (
  vendor_id TEXT,
  tpep_pickup_datetime TEXT,
  tpep_dropoff_datetime TEXT,
  passenger_count TEXT,
  trip_distance TEXT,
  pickup_longitude TEXT,
  pickup_latitude TEXT,
  ratecode_id TEXT,
  store_and_fwd_flag TEXT,
  dropoff_longitude TEXT,
  dropoff_latitude TEXT,
  payment_type TEXT,
  fare_amount TEXT,
  extra TEXT,
  mta_tax TEXT,
  tip_amount TEXT,
  tolls_amount TEXT,
  improvement_surcharge TEXT,
  total_amount TEXT
);

COPY trips FROM '/media/brycemcd/diskc/taxi/newcapture/2016/yellow_tripdata_2016-06.csv' WITH HEADER CSV;

ALTER TABLE trips ALTER COLUMN pickup_longitude SET DATA TYPE NUMERIC(15, 10) USING pickup_longitude::numeric(15,10)
ALTER TABLE trips ALTER COLUMN pickup_latitude SET DATA TYPE NUMERIC(15, 10) USING pickup_latitude::numeric(15,10);
ALTER TABLE trips ALTER COLUMN dropoff_longitude SET DATA TYPE NUMERIC(25, 15) USING dropoff_longitude::numeric(25,15);
ALTER TABLE trips ALTER COLUMN dropoff_latitude SET DATA TYPE NUMERIC(25, 15) USING dropoff_latitude::numeric(25,15);
ALTER TABLE trips ALTER COLUMN fare_amount SET DATA TYPE NUMERIC(15, 3) USING fare_amount::numeric(15,3);
ALTER TABLE trips ALTER COLUMN trip_distance SET DATA TYPE NUMERIC(15, 3) USING trip_distance::numeric(15,3);
ALTER TABLE trips ALTER COLUMN tip_amount SET DATA TYPE NUMERIC(15, 3) USING tip_amount::numeric(15,3);
ALTER TABLE trips ALTER COLUMN tolls_amount SET DATA TYPE NUMERIC(15, 3) USING tolls_amount::numeric(15,3);
ALTER TABLE trips ALTER COLUMN total_amount SET DATA TYPE NUMERIC(15, 3) USING total_amount::numeric(15,3);



SELECT
  ratecode_id
  , AVG(pickup_longitude) as avg_pickup_longitude
  , STDDEV(pickup_longitude) AS stddev_pickup_longitude

  , AVG(pickup_latitude) AS avg_pickup_latitude
  , STDDEV(pickup_latitude) AS stddev_pickup_latitude

  , AVG(dropoff_longitude) AS avg_dropoff_longitude
  , STDDEV(dropoff_longitude) AS stddev_dropoff_longitude

  , AVG(dropoff_latitude) AS avg_dropoff_latitude
  , STDDEV(dropoff_latitude) AS stddev_dropoff_latitude

  , AVG(fare_amount) AS avg_fare_amount
  , STDDEV(fare_amount) AS stddev_fare_amount

  , AVG(trip_distance) AS avg_trip_distance
  , STDDEV(trip_distance) AS stddev_trip_distance

  , AVG(tip_amount) AS avg_tip_amount
  , STDDEV(tip_amount) AS stddev_tip_amount

  , AVG(tolls_amount) AS avg_tolls_amount
  , STDDEV(tolls_amount) AS stddev_tolls_amount

  , AVG(total_amount) AS avg_total_amount
  , STDDEV(total_amount) AS stddev_total_amount

FROM trips
GROUP BY ratecode_id
;
```

output:

-[ RECORD 1 ]------------+----------------------------------
ratecode_id              | 3
avg_pickup_longitude     | -72.6815765930854246
stddev_pickup_longitude  | 9.70795138221968706927
avg_pickup_latitude      | 40.0324579766968867
stddev_pickup_latitude   | 5.34712112492151490096
avg_dropoff_longitude    | -72.9405476483548915
stddev_dropoff_longitude | 9.405902569535654375777774145993
avg_dropoff_latitude     | 40.0325863478338968
stddev_dropoff_latitude  | 5.162215075640283323034859263462
avg_fare_amount          | 65.7217260644289254
stddev_fare_amount       | 18.9712378631904157
avg_trip_distance        | 16.2791899076368551
stddev_trip_distance     | 6.4670774188413505
avg_tip_amount           | 10.1079472854246452
stddev_tip_amount        | 8.8990890068966310
avg_tolls_amount         | 12.9113417436359540
stddev_tolls_amount      | 7.6486037313688523
avg_total_amount         | 89.2588569497634602
stddev_total_amount      | 27.8185977938540479
-[ RECORD 2 ]------------+----------------------------------
ratecode_id              | 1
avg_pickup_longitude     | -73.0990629398085066
stddev_pickup_longitude  | 8.00520984472584521202
avg_pickup_latitude      | 40.2694071071797790
stddev_pickup_latitude   | 4.40992928225504514298
avg_dropoff_longitude    | -73.1584180088748930
stddev_dropoff_longitude | 7.726215202671486641456936168092
avg_dropoff_latitude     | 40.3031498372982584
stddev_dropoff_latitude  | 4.256322171759584352528712054801
avg_fare_amount          | 12.2921834784805965
stddev_fare_amount       | 276.435135825115
avg_trip_distance        | 2.6638182373430271
stddev_trip_distance     | 22.0007280185743064
avg_tip_amount           | 1.6824316968966781
stddev_tip_amount        | 2.2557777668655339
avg_tolls_amount         | 0.20964097976321116234
stddev_tolls_amount      | 1.2408275480925973
avg_total_amount         | 15.3175169411925908
stddev_total_amount      | 276.706655404693
-[ RECORD 3 ]------------+----------------------------------
ratecode_id              | 2
avg_pickup_longitude     | -72.6636663975688965
stddev_pickup_longitude  | 9.34167419636379084136
avg_pickup_latitude      | 40.0286748349555992
stddev_pickup_latitude   | 5.14611785805145795733
avg_dropoff_longitude    | -72.7007449357019364
stddev_dropoff_longitude | 9.370432949491386494452609451418
avg_dropoff_latitude     | 40.0499897675793110
stddev_dropoff_latitude  | 5.162110209797183295537934533461
avg_fare_amount          | 52.0667114819602774
stddev_fare_amount       | 44.1710647853844528
avg_trip_distance        | 17.1748881576370118
stddev_trip_distance     | 5.2754970174010636
avg_tip_amount           | 7.0256166803776394
stddev_tip_amount        | 5.9982447320268529
avg_tolls_amount         | 4.3049112312453481
stddev_tolls_amount      | 4.4419390042641728
avg_total_amount         | 65.0099826850001959
stddev_total_amount      | 44.8451618214508323
-[ RECORD 4 ]------------+----------------------------------
ratecode_id              | 4
avg_pickup_longitude     | -73.6285274874782844
stddev_pickup_longitude  | 4.08292535482373914735
avg_pickup_latitude      | 40.5780311710966300
stddev_pickup_latitude   | 2.25047090136061861762
avg_dropoff_longitude    | -73.3350932344184693
stddev_dropoff_longitude | 5.409654989304751776322300622134
avg_dropoff_latitude     | 40.5691950075002077
stddev_dropoff_latitude  | 2.994347460741559915658513059276
avg_fare_amount          | 66.2346260587493242
stddev_fare_amount       | 41.6491420428270265
avg_trip_distance        | 17.5559560281131735
stddev_trip_distance     | 10.1414206130096351
avg_tip_amount           | 7.7288628581726437
stddev_tip_amount        | 10.1492572911755116
avg_tolls_amount         | 2.0323121283114075
stddev_tolls_amount      | 4.1785539518145825
avg_total_amount         | 77.1716976031717427
stddev_total_amount      | 48.2399528040900928
-[ RECORD 5 ]------------+----------------------------------
ratecode_id              | 6
avg_pickup_longitude     | -70.1657079060880342
stddev_pickup_longitude  | 16.38359027218949941800
avg_pickup_latitude      | 38.6541019667923077
stddev_pickup_latitude   | 9.02568465725431417185
avg_dropoff_longitude    | -66.3701479007036259
stddev_dropoff_longitude | 22.533889185501331503571659240619
avg_dropoff_latitude     | 36.5623278169550447
stddev_dropoff_latitude  | 12.413590706061778625862369837650
avg_fare_amount          | 3.9100000000000000
stddev_fare_amount       | 10.8837403243680829
avg_trip_distance        | 1.3211965811965812
stddev_trip_distance     | 3.4158343662887470
avg_tip_amount           | 0.98102564102564102564
stddev_tip_amount        | 5.0495188204721435
avg_tolls_amount         | 0.35538461538461538462
stddev_tolls_amount      | 2.1910010066498335
avg_total_amount         | 6.0190598290598291
stddev_total_amount      | 12.1804068206014836
-[ RECORD 6 ]------------+----------------------------------
ratecode_id              | 99
avg_pickup_longitude     | -44.5942101759051471
stddev_pickup_longitude  | 36.25505393828644724545
avg_pickup_latitude      | 24.5694517247812500
stddev_pickup_latitude   | 19.97495863779419720642
avg_dropoff_longitude    | -27.1905980951645795
stddev_dropoff_longitude | 35.725892325429935844718441427987
avg_dropoff_latitude     | 14.9813391601338106
stddev_dropoff_latitude  | 19.684085838324882395282132598814
avg_fare_amount          | 30.7162132352941176
stddev_fare_amount       | 42.4717027219531711
avg_trip_distance        | 1.9577205882352941
stddev_trip_distance     | 7.7780853530466839
avg_tip_amount           | 0.96338235294117647059
stddev_tip_amount        | 3.2438016329730423
avg_tolls_amount         | 0.53261029411764705882
stddev_tolls_amount      | 2.3514122683372642
avg_total_amount         | 32.5927205882352941
stddev_total_amount      | 43.1155589414006096
-[ RECORD 7 ]------------+----------------------------------
ratecode_id              | 5
avg_pickup_longitude     | -61.7473251124014668
stddev_pickup_longitude  | 27.45298574637562436508
avg_pickup_latitude      | 34.0212270734065729
stddev_pickup_latitude   | 15.12592957832707927319
avg_dropoff_longitude    | -66.2493943255697692
stddev_dropoff_longitude | 22.624731594476484597553032814099
avg_dropoff_latitude     | 36.5061803412299397
stddev_dropoff_latitude  | 12.467232619881234386799824826939
avg_fare_amount          | 64.2911196581196581
stddev_fare_amount       | 700.936555452430
avg_trip_distance        | 6.6573010752688172
stddev_trip_distance     | 11.5145767670649298
avg_tip_amount           | 7.0306175902950096
stddev_tip_amount        | 12.4488372069735408
avg_tolls_amount         | 3.4189751861042184
stddev_tolls_amount      | 7.0081619007364514
avg_total_amount         | 75.1750107526881720
stddev_total_amount      | 701.543624042680


```sql
SELECT ratecode_id, COUNT(*) FROM trips GROUP BY ratecode_id;
```

```
 ratecode_id |  count   
-------------+----------
 3           |    22195
 1           | 10815797
 2           |   255270
 4           |     5549
 6           |      117
 99          |      272
 5           |    36270
```
