## Global-DB

This module has a lot in common with [Global-FS module](https://github.com/softindex/datakernel/tree/master/global-fs). 
It has the the same network topology and similar algorithms for data authentication. The core difference is that Global-DB 
is optimized for storing small binary key-value pairs. In accordance to the purpose, Global-DB uploads and downloads files
without Streams.

To see how the technology works, you can run [Global-DB demo application](https://github.com/softindex/datakernel/tree/master/examples/global-db-demo).