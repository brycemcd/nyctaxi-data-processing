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

For the purposes of analysis, data is processed for inclusion of
discrete values and correctness of continuous data against extreme values.
Correctness refers to its conformity to the data
dictionary published by the Taxi and Limousine Commission. Extreme
values, for now, refer to values of an individual real value being more than
three standard deviations from it's mean. This is a naive point of view
for now. After some initial analysis the definition of extreme may
change.

### Continuous Value Validations

In this version, I'm calculating important numeric properties of validation
offline and storing the results as a configuration. I'm deferring a more
complicated and programatic approach for a future version.

As I contemplated how to do this, I realized that a shifting window will
eventually be required to properly compare individual trip rows and assess
their validity. As an example, a data point taken in 2014 (before the explosion
of Uber) will likely have an overall shorter trip time and thus a different mean.
Comparing a more recent trip time against an older trip time would yield an
insufficient comparison and result in far more false negative validations

For now, keeping it simple and pretending that no drift in the data exists.
I'm fixing other defects in the software and concentrating on creating
the proper interfaces before tackling this problem.
