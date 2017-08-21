(ns taxidata.core-test
  (:require [clojure.test :refer :all]
            [taxidata.core :refer :all]
            [taxidata.scratch :refer :all]
            [taxidata.input-impl-file :refer :all]
            [taxidata.util-calc :as calc]
            [bond.james :as bond :refer [with-spy with-stub!]]))

;; NOTE: 99 records is helpful here to avoid influencing the mean and stddev
(def all-returns (atom ()))
(defn callbk
  [trip]
  (swap! all-returns conj trip))

(def validrecords99 (reverse (reduce conj
                                     ()
                                     (create-trips-from-file filename99 callbk))))

(def first-row (last @all-returns))

(deftest invalidate-test
  (with-test
    (def example-row {:foo "bar"})
    (def invalid-col :key1)
    (def invalid-col2 :key2)

    (testing "assoc :valid false to the map"
      (is (= false (:valid (invalidate example-row invalid-col)))))

    (testing "when no invalid reasons exist, A new key and list are created"
      (let [return-map (invalidate example-row invalid-col)
            invalid-reasons (:invalid-reason return-map)]
        (is (= invalid-col (first invalid-reasons)))
        (is (= 1 (count invalid-reasons)))))

    (testing "when invalid reasons already exist, A new key and vector are created"
      (let [return-map (invalidate (assoc example-row :valid false :invalid-reason (cons invalid-col2 '())) invalid-col)
            invalid-reasons (:invalid-reason return-map)]
        (is (= invalid-col (first invalid-reasons)))
        (is (= invalid-col2 (second invalid-reasons)))
        (is (= 2 (count invalid-reasons)))))))

(deftest audit-continuous-key-test
  (def numeric-column :tip_amount)
  (testing "base case. A tip_amount within 3 stddevs should be considered
           reasonable"
    (is (= nil (:valid (audit-continuous-key first-row numeric-column)))))

  (testing "when a numeric is extreme, :valid false should be applied"
    (with-stub! [[calc/extreme-numeric? (fn [_ _ _] true)]]
      (let [returned-trip (audit-continuous-key first-row numeric-column)]
        (is (= numeric-column (first (:invalid-reason returned-trip))))
        (is (= false (:valid returned-trip)))))))

(deftest validate-trip-continuous-test
  (testing "calls audit-continuous-key for each key in the trip for each validation
           specified as a numeric-validation"
  (with-spy [audit-continuous-key]
      (validate-trip-continuous first-row)
    (let [calls (bond/calls audit-continuous-key)]
      (is (= (count calls) (count (keys (last (first numeric-validations))))))))))

(with-test
  (def validation-column :vendor_id)
  (def valid-set #{1 2})
  (def enum-validations-test
    {:vendor_id #{1 2}})

  (testing "audit-enum-key"
    (testing "invalidates trip when key does not exist (should be a limited
             case now that this uses defrecord"
      (is (= false (:valid (audit-enum-key {:foo "bar"} validation-column valid-set)))))

    (testing "invalidates the trip when the value is not in the verification set"
      (is (= false (:valid (audit-enum-key {validation-column (inc (last valid-set))} validation-column valid-set)))))

    (testing "does not invalidate when the value is in the validation set"
      (is (= nil (:valid (audit-enum-key {validation-column (last valid-set)} validation-column valid-set))))))

    (deftest validate-trip-enum-keys-test
      (testing "calls audit-enum-key for each key in the trip for each validation
               specified in the validations variable"
      (with-spy [audit-enum-key]
          (validate-trip-enum {:vendor_id 2} enum-validations-test)
        (let [calls (bond/calls audit-enum-key)]
          (is (= (count calls) (count (keys enum-validations-test)))))))))

(deftest dropoff-after-pickup?-test
  (with-test
    (def validdropoff {:tpep_pickup_datetime  (to_dttm "2017-07-02 18:00:00")
                       :tpep_dropoff_datetime (to_dttm "2017-07-02 19:00:00")})

    (testing "dropoff-after-pickup? validation to ensure dropoff time is later than pickup time"
      (is (= true (dropoff-after-pickup? validdropoff)))))

  (with-test
    (def invaliddropoff {:tpep_pickup_datetime  (to_dttm "2017-07-02 18:00:00")
                         :tpep_dropoff_datetime (to_dttm "2017-07-02 17:00:00")})

    (testing "dropoff-after-pickup? validation to ensure dropoff time is later than pickup time"
      (is (= false (dropoff-after-pickup? invaliddropoff))))))

; FIXME: for reasons I can't comprehend, audit-row-relationship-test NEVER returns failed tests (false positive!)
(deftest validate-trip-test
  (with-test
    (def dropoff-before-pickup {:tpep_pickup_datetime  (to_dttm "2017-07-02 18:00:00")
                                :tpep_dropoff_datetime (to_dttm "2017-07-02 14:00:00")})

    (def dropoff-after-pickup  {:tpep_pickup_datetime  (to_dttm "2017-07-02 18:00:00")
                                :tpep_dropoff_datetime (to_dttm "2017-07-02 20:00:00")})
    (testing "row is invalidated if validation functions return false"
      (is (= true (contains? (validate-trip-relationship dropoff-before-pickup) :valid))))

    (testing "row is validated if validation functions return true"
      (is (= false (contains? (validate-trip-relationship dropoff-after-pickup) :valid))))))

(deftest validate-trip-test
  (testing "base case, validations pass and :valid is applied to trip"

    (def valid-row
      (assoc first-row
             :dropoff_latitude (get-in numeric-validations [1 :dropoff_latitude :mean])
             :pickup_latitude (get-in numeric-validations [1 :pickup_latitude :mean])))

    (is (= false (:valid (validate-trip (invalidate valid-row :because)))))
    (is (= true (:valid (validate-trip valid-row)))))

  (testing "all validation functions are called"
    (with-spy [validate-trip-relationship validate-trip-enum validate-trip-continuous]
      (validate-trip first-row)
      (is (= 1 (count (bond/calls validate-trip-enum))))
      (is (= 1 (count (bond/calls validate-trip-continuous))))
      (is (= 1 (count (bond/calls validate-trip-relationship)))))))
