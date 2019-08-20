---
id: fs
filename: fs
title: FS Module
prev: cloud/rpc.html
next: cloud/ot.html
nav-menu: cloud
layout: cloud

---

FS Module is basis for building efficient, scalable remote file storage. supporting data redundancy, rebalancing and 
resharding. The key principle is effective data immutability, which enables simple design, low-overhead and aggressive 
caching. This technology allows to create high-throughput data storage and processing applications.
All data in FS is organized in file systems.

You can add FS module to your project by inserting dependency in `pom.xml`: 

{% highlight xml %}
<dependency>
    <groupId>io.datakernel</groupId>
    <artifactId>datakernel-fs</artifactId>
    <version>{{site_datakernel_version}}</version>
</dependency>
{% endhighlight %}

This module includes **FsClient** interface which represents a client with *upload*, *download*, *move*, *delete* and 
*list* operations. There are several implementations of the interface:

* **LocalFsClient** - a client which works with local file system and doesn't involve working with networks.
* **RemoteFsClient** - connects to a RemoteFsServer and communicates with it.
* **RemoteFsClusterClient** - a client which operates on a map of other clients as a cluster.

Also, there is a **RemoteFsServer** which is an implementation of **AbstractServer** for Cloud-FS.

### Examples
1. [Server Setup Example](https://github.com/softindex/datakernel/blob/master/examples/cloud/fs/src/main/java/ServerSetupExample.java) - 
configuring and launching `RemoteFsServer`.
2. [File Upload Example](https://github.com/softindex/datakernel/blob/master/examples/cloud/fs/src/main/java/FileUploadExample.java) - 
uploading file to `RemoteFsServer`.
3. [File Download Example](https://github.com/softindex/datakernel/blob/master/examples/cloud/fs/src/main/java/FileDownloadExample.java) - 
downloading file from `RemoteFsServer`.

To run the examples in an IDE, you need to clone DataKernel:
{% highlight bash %}
$ git clone https://github.com/softindex/datakernel
{% endhighlight %}

And import it as a Maven project. Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

First, open `ServerSetupExample` class which is located at **datakernel -> examples -> cloud-> fs**, and run its *main()* 
method.

Then, open `FileUploadExample` class which is located in the same directory, and run its *main()* method. 

Finally, open `FileDownloadExample` class which is located in the same directory, and also run its *main()* method.

In the example we upload file "example.txt" to server and then download it back as "download_example.txt".

Let's have a closer look at **Server Setup Example**. To make setup and launching as simple as possible, there is a 
special `RemoteFsServerLauncher` from Launchers module. It allows to setup FS server in less then 30 lines of code:

{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/cloud/fs/src/main/java/ServerSetupExample.java tag:EXAMPLE %}
{% endhighlight %}

**File upload** and **download** examples have alike implementations. Both of them extend `Launcher` and thus 
implement *run()* method which defines the main behaviour of the launcher.
Also, both of the examples utilize CSP module - uploader uses `ChannelFileReader` while downloader uses `ChannelFileWriter`. 
They allow to asynchronously read/write data from/to files. 
