(defproject taxidata "0.0.1"
  :description "Cleaning Taxi data (related to https://github.com/brycemcd/NYC-taxi-time-analysis)"
  :url "https://github.com/brycemcd/NYC-taxi-time-analysis"
  :license {:name "MIT"}
  :jvm-opts ["-Xmx15G"]
  :plugins [[lein-auto "0.1.3"]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clj-time "0.13.0"]
                 [kixi/stats "0.3.8"]
                 [circleci/bond "0.3.0"]
                 [cprop "0.1.11"]
                 ])
