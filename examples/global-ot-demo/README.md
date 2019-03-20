## Global-OT demo application

To understand how Global-OT module works and organizes data, you can run Global-OT demo application. It provides you with 
ability to work with repository which represents the history of counter modifications and covers the following operations:
* `Add` - saves your modification locally, so that it can be then committed and pushed.
* `Push` - applies your changes to the commit graph.
* `Pull` - updates your repository by downloading operations which were created by other participants. If there are 
several heads, it will merge them into one and then move your changes on top of it.
* `Merge Heads` - merges all available heads and receives an automatically-determined single result.
* `Reset` - deletes all your uncommitted operations.

As you have probably noticed, these operations resemble Git API. The main advantage of Global-OT over Git is that there 
are no conflicts while merging - you receive a single consistent result by simply clicking `Merge Heads` button without 
any additional manual conflict resolution. Global-OT has already done it for you.

Also, demo application has an additional operation:
* `New Manager` - adds new virtual participant to repository (will be opened in new browser tab).

There are two values which help to work with the repository:
* `Remote state` - shows remote value of the counter. 
* `Uncommitted operations` - represents operations which were `Add`ed but not committed yet.

All of the pushed commits are displayed in commit graph. Each commit is represented by hashcode of the operation and
each edge on the graph represents applied operations. 

You can run this application in **5 steps**:

#### 1. Clone DataKernel project
You can clone the project either in console:
```
$ git clone https://github.com/softindex/datakernel.git
```
Or with IDE tools.

#### 2. Set up the project

If you'd like to run the example in console, you need to install DataKernel:
```
$ cd datakernel
$ mvn clean install -DskipTests
```

To run the example in an IDE, set up default working directory of run configurations in your IDE so that the example can 
work correctly. In accordance to DataKernel module structure, the working directory should be set to the module folder. 

In IntelliJ IDEA you can do it in the following way:
`Run -> Edit configurations -> |Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> 
$MODULE_WORKING_DIR$||`.

Then build the project (**Ctrl + F9** for IntelliJ IDEA).

#### 3. Run Global Launchers

In console:
```
$ cd datakernel/global-launchers
$ mvn exec:java@DiscoveryServiceLauncher
$ # start another process
$ cd datakernel/global-launchers
$ mvn exec:java@GlobalNodesLauncher
```
In an IDE:

First, open *DiscoveryServiceLauncher* class, which is located at **datakernel -> global-launchers -> ... -> launchers** 
and run its *main()* method.

Next, open *GlobalNodesLauncher* class, which is located at **datakernel -> global-launchers -> ... -> discovery** and 
run its *main()* method.

#### 4. Run Global-OT demo app
Note: **Do not terminate Launchers**.

To run the app in console enter these commands:
```
$ cd datakernel/examples/global-ot-demo
$ mvn exec:java@GlobalOTDemoApp
```
In IDE:
Open `GlobalOTDemoApp` class, which is located at **datakernel -> examples -> global-ot-demo** and run its 
*main()* method.

#### 5. Open your favourite browser

After you've started all of the needed classes, open your browser and go to [localhost:8899](http://localhost:8899). Enjoy!