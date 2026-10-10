# Live sync

Tags: sdk

* Create a fresh sync database
* Create a sync device "live"

## A continuous live sync receives server changes
* Start continuous live sync for device "live"
* Save document "remote" on the server with field "v" = "1"
* Device "live" receives document "remote" within "15" seconds
* Stop live sync for device "live"

## A continuous live sync pushes local edits without a manual sync
* Start continuous live sync for device "live"
* Device "live" saves document "local" with field "v" = "2"
* The server receives document "local" within "15" seconds
* Stop live sync for device "live"
