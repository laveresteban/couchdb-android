# Sync progress

Tags: sdk

* Create a fresh sync database
* Create a sync device "reader"

## Pulls report how many changes the server still has
* Server has "5" documents named "item"
* Device "reader" syncs in batches of "2" and records progress
* Recorded progress includes pulling "2" documents with "3" pending
* Recorded progress includes pulling "5" documents with "0" pending
