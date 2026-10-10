# Compound indexes

Tags: sdk

* Create a fresh sync database
* Create a sync device "d"

## A sort on the indexed fields comes back in index order
* Device "d" saves document "a" with team "x" and score "2"
* Device "d" saves document "b" with team "x" and score "1"
* Device "d" saves document "c" with team "w" and score "3"
* Device "d" saves document "e" with team "x" and score "10"
* Device "d" creates a compound index on "team,score"
* Device "d" lists documents sorted by "team,score" ascending as "c,b,a,e"
* Device "d" lists documents sorted by "team,score" descending as "e,a,b,c"

## Updates move documents within the index
* Device "d" saves document "a" with team "x" and score "2"
* Device "d" saves document "b" with team "x" and score "1"
* Device "d" creates a compound index on "team,score"
* Device "d" saves document "b" with team "x" and score "5"
* Device "d" lists documents sorted by "team,score" ascending as "a,b"
