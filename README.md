# nyctaxi-data-processing
I'm just learning clojure and using the NYC taxi data set to learn clojure
idioms and clean up the data set.

## Dataset

This dataset was obtained from
http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml on or
around September 3rd, 2016. Subsequent downloads of more recent data
have been captured.

The focus of this project is processing the yellow cab trip data. A data
dictionary for this set can be [found at
nyc.gov](http://www.nyc.gov/html/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).

## Validity

For the purposes of analysis, data is processed for correctness and
extreme values. Correctness refers to its conformity to the data
dictionary published by the Taxi and Limousine Commission. Extreme
values, for now, refer to values of an individual real value being more than
three standard deviations from it's mean. This is a naive point of view
for now. After some initial analysis the definition of extreme may
change.
