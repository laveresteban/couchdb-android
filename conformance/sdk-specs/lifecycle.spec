# Closing a database

Tags: sdk

* Create a sync device "phone"

## A closed database refuses reads
* Device "phone" saves document "a" with field "v" = "1"
* Close device "phone"
* Device "phone" fails to read document "a"
