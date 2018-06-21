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

## Invocation

This code is meant to be run from the command line. It has been tested
on linux (Ubuntu) systems with the following java build:

```java version
"1.8.0_131"
Java(TM) SE Runtime Environment (build 1.8.0_131-b11)
Java HotSpot(TM) 64-Bit Server VM (build 25.131-b11, mixed mode)
```

`java -jar target/taxidata-<version>-standalone.jar yellow_tripdata_2016-06.csv valid.csv invalid.csv`

`yellow_tripdata_2016-06.csv` is a downloaded csv from TLCs download site

`valid.csv` is a file where valid data will be stored

`invalid.csv` is a file where trips that are deemed to be invalid are
stored

Create the jar using lein if it is not present in `target/`: `lein uberjar`

## Running Tests



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

## Validation Notes


### 2017-07-05
Running the above frequencies function yeilds this:

```clojure
{
:tip_amount 42658,
:tolls_amount 3212
:extra 449637
:improvement_surcharge 459
:pickup_longitude 12566
:ratecode_id 16
:trip_distance 29314
:dropoff_longitude 11659
:fare_amount 13
:mta_tax 4987
:total_amount 20
:whole-row-validation 1109}
```
the "extra" column is an enum with recorded valid values of 0.5 and 1.0

```clojure
(map println (sort-by last (frequencies (map :extra records1M))))
[2.0 1]
[0.3 1]
[70.0 1]
[2.5 1]
[10.0 1]
[20.0 1]
[1.5 1]
[50.0 1]
[30.3 1]
[0.8 1]
[34.59 1]
[0.03 1]
[1.23 1]
[4.54 1]
[0.02 2]
[5.5 3]
[0.1 4]
[-4.5 7]
[-1.0 55]
[-0.5 181]
[4.5 4252]
[1.0 157483]
[0.5 392880]
[0.0 445119]
```

I think 0.0 is a valid value and I have no idea what the other values could be
from the numeric validation
(frequencies (flatten (map :invalid-reason (filter #(= false (:valid %)) (validate-rows records1M)))))

Much better. The next highest invalid column is tip_amount
tip_amount is a continuous column. The mean of the invalid rows is 12.985 with
stddev of 7.27 That doesn't seem that high to me. It's reasonable that someone
was coming to/from the airport. $90 fare * 20% tip would be an $18 tip
the mean tip amount of valid values is 1.6578 with a stddev of 1.8384
just out of sheer curiosity, the mean trip distance for valid tip_amounts is
2.7700 and the mean trip distance of invalid tip_amounts is 15.103. I think
this is a false positive. There are probably enough long trips to justify the
large tip amount. I'll move the valid numeric out to 4 stddev to see what that
does

Still makes sense. Mean fare_amount = 58.3366 with a mean tip amount of 17.36
for the invalid tips. That's a 30% tip and well within reason

Even at 5 stddev as the definition of extreme numeric, the mean trip_distance
is 16.0377, 22.344 mean tip and 67.744 mean fare_amount. This is pretty reasonable
I think it would be fairly inconsequential to exclude these trips for now
and come up with a fare / distance / tip algo to detect extreme values. It's
likely a multi-modal distribution and auditing the data should appreciate that
phenomenon


### 2017-08-20

Create numeric-validations.edn after some analysis revealed that
grouping the continuous values by `ratecode_id` reduces the false
positives markedly.

Running the code on 1M records shows ~64k invalid records. ~25k of these
invalid records are based only on the :tolls_amount validation. Most of
those 25k values are false positive. Some analysis shows that if I ungroup on
ratecode_id that there will be many fewer false positives.
Alternatively, I could add 0.0 as an acceptable value and remove 0's
from the mean/stddev calculation and calculate P(extreme| toll > 0).

The next highest source of invalid records was the combination of tolls
and tip amount. Looking through the data filtered on this error, it
appears there are MANY reasonable amounts here. It's possible that
adding some logic as in the above would help in reducing false
positives.

Another large source of false positives appears to be on `mta_tax`.
Ratecode = 3 is a flat fare to Newark where no tax is collected. This is
true for every invalid record.

I was able to find [a copy of the MTA tax law (albeit from
2012)](http://www.nyc.gov/html/tlc/downloads/pdf/taxi_fare_rules_passed.pdf)
indicating that MTA tax should only be collected when a fare is
initiated in NYC and ends in one of these places:
New York City

* Dutchess County
* Nassau County
* Orange County
* Putnam County
* Rockland County
* Suffolk County
* Westchester County

A validation should be added to accept 0 as an acceptable enum value and
also check that tax was collected according to the above rules. (I
probably want to accept the record as valid when checking against the
entire rule set and mark it as some sort of cheater if tax isn't being
collected when it should be.)

The more I'm observing the error trends of this data, the more I see
that validity is based on several forks of if/then logic. If a trip
originated at LGA and went to Westchester, it's perfectly valid that the
total amount would be high and have a lot of tolls applied. This is a
reasonable, yet rare, case. I'm questioning applying an ML algo to these
data to help the validation process.

Having a ratecode_id of 1 and a standard deviation of 276 allows for almost all
fares to be valid on the basis of `total_amount`.
