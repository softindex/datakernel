The main features of Global-FS are:
* All data is organized in file systems which have a corresponding pair of public and private keys
* File systems can be accessed by unique public key
* Only owner of private key can add files to the file systems
* Files can optionally be encrypted
* Files (and even their parts) authenticity can be verified with public key

Global-FS has a fully decentralized multi-tier network which enables:
* High fault tolerance 
* Caching and prefetching
* Natural network scalability under load - the more a certain file system is accessed, the more its cached copies are 
created
* Global-FS servers can be installed near data consumers (in the same datacenter, on ISP last mile, or even embedded 
directly into application) - which means extremely low latencies and high data availability.

You can run [demo FS application](https://github.com/softindex/datakernel/tree/master/examples/global-fs-demo) to see 
how the technology works.