## Global-FS

This module enables creating worldwide storage clouds with implementation of digital signatures and caching technologies.

The main features of Global-FS are:
* All data is organized in file systems which have a corresponding pair of public and private keys
* File systems can be accessed by unique public key
* Only owner of private key can add files to the corresponding file system
* Files can optionally be encrypted
* Files (and even their parts) authenticity can be verified with public key

### Global-FS has a fully decentralized multi-tier network which enables:
* High fault tolerance 
* Caching and prefetching
* Natural network scalability under load - the more a certain file system is accessed, the more its cached copies are 
created
* Global-FS servers can be installed near data consumers (in the same datacenter, on ISP last mile, or even embedded 
directly into application) - which means extremely low latencies and high data availability.

### Global-FS has a multi-tier network topology:

<img src="http://datakernel.io/static/images/globalfs-architecture.png">

1. Client applications layer - all Global-FS applications, such as mobile and desktop applications or even a server.
2. P2P Global-FS servers layer which can be split in two logical layers:
    * Master FS-Servers (store original file systems)
    * Caching FS-Servers (store cache of file systems)
3. P2P Discovery Service layer - has a DNS-like role, stores information about IPs and public keys of the uploaded 
file systems.

### Encryption and security
1. Each file in Global-FS is automatically signed with private key and can optionally be encrypted.
2. Anyone who downloads file can check the validity of signatures. Moreover, Caching servers can check
the validity even if the file is encrypted.
3. Each uploaded to Global-FS network file is divided in parts which consist of *checkpoints* and particular amount of 
bytes of data (amount can be configured using FS-Driver). These *checkpoints* are signed and control if any data was modified.


### Data sharing in Global-FS
#### Uploading files

* If users want to upload some file systems to Global-FS, a pair of public and private keys is generated. Now their files 
can be signed and identified.
* Uploaders can optionally encrypt their files with self-generated symmetric encryption key.
* Uploaders should decide where to upload their data. There are three most possible options:
    * upload to their own Global-FS Servers;
    * reach an agreement with another Global-FS Server owner (for example, pay for usage of server powers);
    * use a free public Global-FS Server.
    
    Or they can create their own option.
    
    The chosen servers become Master servers, a primary source of the data.
* FS-Driver announces the Discovery Service via FS-Servers that users want to upload some data and which servers are 
chosen as Master. Discovery Service checks the signature of the announce package and if this stage completes successfully, 
the uploaded files can be reached by any user.

#### Downloading files
In order to download some files, users need to know the public key of the file system. If the files are encrypted, 
downloaders should also know the symmetric encryption key in order to read them. 

* Downloaders connect to any FS-Server to get the file system they need. For this purpose mostly one of the following 
options is used:

    * Local datacenter
    * Physically most closely located server
    * ISP Server
* If the server contains requested file system and it is relevant (most recent, the biggest), downloaders get it and the 
process completes. But if the data or its more relevant version is located somewhere else, server sends a request to a 
Discovery Service server.
* Discovery Service redirects the request to the Master server which has the needed file system and downloading starts.
* While downloading, the server to which users first connected to will save the cache of all the transited files (if 
caching is enabled).

After successful download, users can check data validity by signature.

You can run [demo FS application](https://github.com/softindex/datakernel/tree/master/examples/global-fs-demo) to see 
how the technology works.