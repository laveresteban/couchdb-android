# Large attachments

Tags: sdk

* Create a fresh sync database
* Create a sync device "phone"
* Create a sync device "tablet"

## Attachments over the inline limit stream from one device to another
* Device "phone" saves document "photo" with a "2097152"-byte attachment "scan.bin"
* Device "phone" syncs with the server
* Server attachment "scan.bin" of document "photo" has "2097152" bytes
* Device "tablet" syncs with the server
* Device "tablet" has a "2097152"-byte attachment "scan.bin" on document "photo"
