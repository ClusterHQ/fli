# 0.7.0 (2016-12-06)

### Features
* Performance improvement when cloning snapshots. Added upgrade code to migrate from versions 0.6.0 or lower.
* Improved volumeset by doing all the lookups on the database instead of fetching it over the wire.

## 0.6.0 (2016-11-29)

### Features
* Added new command `fli fetch` to fetch metadata from FlockerHub

### Bug Fixes
* Fixes name validation when doing updates

## 0.5.0 (2016-11-21)

## 0.4.0 (2016-11-18)

### Features
* Added new HTTP request to gather fli events for analyics.

## 0.3.0 (2016-11-17)

## 0.2.0 (2016-11-08)

### Bug Fixes
* Fixes panic when running update snapshot.

### Features
* Added reference docs URLs to guide user for specific errors like missing ZFS components.
* Added `fli sync --all` to sync all volumesets on the client
