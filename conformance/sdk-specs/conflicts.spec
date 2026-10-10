# Conflicts on a device

Tags: sdk

* Create a fresh sync database
* Create a sync device "a"
* Create a sync device "b"

## The replicator settles a conflict right after pulling it
* Device "a" saves document "n" with field "v" = "base"
* Device "a" syncs with the server
* Device "b" syncs with the server
* Device "a" saves document "n" with field "v" = "alpha"
* Device "a" syncs with the server
* Device "b" saves document "n" with field "v" = "beta"
* Device "b" syncs and resolves conflicts preferring field "v" = "alpha"
* Device "b" has "0" conflicts for document "n"
* Device "b" has document "n" with field "v" = "alpha"
* Device "b" syncs with the server
* Server document "n" has field "v" = "alpha"

## Conflicts can be listed and settled by hand
* Device "a" saves document "n" with field "v" = "base"
* Device "a" syncs with the server
* Device "b" syncs with the server
* Device "a" saves document "n" with field "v" = "alpha"
* Device "a" syncs with the server
* Device "b" saves document "n" with field "v" = "beta"
* Device "b" syncs with the server
* Device "b" has "1" conflicts for document "n"
* Device "b" resolves document "n" by keeping field "v" = "beta"
* Device "b" has "0" conflicts for document "n"
* Device "b" has document "n" with field "v" = "beta"
