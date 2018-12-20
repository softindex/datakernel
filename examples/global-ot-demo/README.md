## Global-OT demo application
To understand how Global-OT works and organizes data you can run Global-OT demo application. It provides you with ability 
to work with repository which represents the history of counter modifications and covers the following operations:
* Add - saves your modification locally, so that it can be then committed.
* Commit - locally creates a commit which then can be pushed to the repository.
* Push - applies your commit to the commit graph.
* Fetch - updates your repository by downloading operations which were created by other participants.
* Rebase - assume you've done some local changes and have not committed them yet. Then you fetched changes and it 
turned out that you're lagging behind the newest commit (HEAD). To switch to the commit you can simply click Rebase - this
operation will rebase you to the relevant commit while saving all of your local changes.
* Pull - basically, it is a sequence of `Fetch` and `Rebase` operations.
* Merge Heads - merges all available heads and receives an automatically-determined single result.
* Reset - deletes all your uncommitted operations.

As you have probably noticed, these operations resemble Git API. The main difference is that there are no conflicts while 
merging - you receive a single consistent result by simply clicking `Merge Heads` button without any additional manual 
conflict resolution. Global-OT has already done it for you.

Also, demo application has an additional operation:
* New Manager - adds new virtual participant to repository (will be opened in new browser tab).

There are two markers which help to work with the repository:
* Current state - shows local value of the counter. 
* Uncommitted operations - represents operations which were `Add`ed but not committed yet.

All of the pushed commits are represented in commit graph. Each commit contains the following information:
* Hashcode of the operation
* Current value of the counter

To run the Global-OT demo application, you should first run `DiscoveryServiceLauncher` and `GlobalNodesLauncher` and 
after that run `GlobalOTDemoApp`. To execute this enter the following commands:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel/global-launchers
$ mvn clean compile exec:java@DiscoveryServiceLauncher
$ #in another console
$ cd datakernel/global-launchers
$ mvn clean compile exec:java@GlobalNodesLauncher
$ #in another console
$ cd datakernel/examples/global-ot-demo
$ mvn clean compile exec:java@GlobalOTDemoApp
```
Note that it is important to execute commands in the given order.

After you enter the commands, open your browser and go to localhost:8899. Enjoy!