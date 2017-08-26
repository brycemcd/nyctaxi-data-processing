# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]
### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security

## 0.2.0 - 2017-08-26

### Added

- numeric-validation.edn to house data for continuous validation
- TaxiTrip defrecord - each non-header row in the data file is a record
- command line invocation
- first-pass at testing major components. Far from complete or
  comprehensive

### Changed
- Reduces disk IO markedly by changing all validation functions to be
  row based: All validation functions take a single row and invalidate
  if necessary
- continuous validation grouped by `ratecode_id`
- file organization (I read "Clojure Applied" and followed advice)
- Updated README with validation log + invocation examples

### Deprecated
### Removed
### Fixed
### Security
### Benchmark

On my test box, filename1000000 is processed:

144 seconds
34994 invalid records


[Unreleased]: https://github.com/your-name/taxidata/compare/0.2.0...HEAD
[0.0.1]: https://github.com/brycemcd/taxidata/compare/0.2.0...0.0.1

## 0.0.1 - 2017-06-04

### Added
- Project initiated
- Reads entire file and processes basic validation steps end to end
