(defproject taxidata "0.2.0"
  :description "Cleaning Taxi data (related to https://github.com/brycemcd/NYC-taxi-time-analysis)"
  :url "https://github.com/brycemcd/NYC-taxi-time-analysis"
  :license {:name "MIT"}
  :plugins [[lein-auto "0.1.3"]]
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [clj-time "0.13.0"]
                 [kixi/stats "0.3.8"]
                 [circleci/bond "0.3.0"]
                 [cprop "0.1.11"]
                 [spootnik/kinsky "0.1.22"]
                 [org.clojure/core.async "0.4.474"] ; reqd?
                 ;; https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
                 [org.apache.kafka/kafka-clients "0.10.2.1"]
                 ]

  :main taxidata.main
  :aot :all
  )
