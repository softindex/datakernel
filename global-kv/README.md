## Global-KV

This module has a lot in common with [Global-FS module](https://github.com/softindex/datakernel/tree/master/global-fs). 
It has the the same network topology and similar algorithms for data authentication. The core difference is that Global-KV 
is optimized for storing small binary key-value pairs. In accordance to the purpose, Global-KV uploads and downloads files
without Streams.

You can explore Global-KV API for clients, storage and nodes [here](https://github.com/softindex/datakernel/tree/master/global-kv/src/main/java/io/global/kv/api).

To see how the technology works, you can run [Global-KV demo application](https://github.com/softindex/datakernel/tree/master/examples/global-kv-demo).
