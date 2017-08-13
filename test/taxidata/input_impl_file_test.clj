(ns taxidata.input-impl-file-test
  (:require [clojure.test :refer :all]
            [taxidata.scratch :refer :all]
            [taxidata.core :refer :all]
            [taxidata.input-impl-file :refer :all]
            [bond.james :as bond :refer [with-spy]]))
(with-test
  (def rows (import-file "data/trip_2016_06-10.csv"))
  (def first-row (first rows))

  (testing "imports 9 rows of valid data"
    (is (= 9 (count rows))))

  (testing "after file is imported, data in file is converted as a map"
    (is (instance? clojure.lang.PersistentHashMap first-row))

    ; NOTE: these are extremely brittle tests, but I'm not sure of another way
    ; of writing these without making them really difficult to read/reason about
    (is (= 2 (:vendor_id first-row)))
    (is (= (to_dttm "2016-06-09 21:06:36") (:tpep_pickup_datetime first-row)))
    (is (= (to_dttm "2016-06-09 21:13:08") (:tpep_dropoff_datetime first-row)))
    (is (= 2 (:passenger_count first-row)))
    (is (= 0.79 (:trip_distance first-row)))
    (is (= -73.983360290527344 (:pickup_longitude first-row)))
    (is (= 40.760936737060547 (:pickup_latitude first-row)))
    (is (= -73.977462768554688 (:dropoff_longitude first-row)))
    (is (= 40.753978729248047 (:dropoff_latitude first-row)))
    (is (= 1 (:ratecode_id first-row)))
    (is (= "N" (:store_and_fwd_flag first-row)))
    (is (= 2 (:payment_type first-row)))
    (is (= 6.0 (:fare_amount first-row)))
    (is (= 0.5 (:extra first-row)))
    (is (= 0.5 (:mta_tax first-row)))
    (is (= 0.0 (:tip_amount first-row)))
    (is (= 0.0 (:tolls_amount first-row)))
    (is (= 0.3 (:improvement_surcharge first-row)))
    (is (= 7.3 (:total_amount first-row)))))
