## Global-OT demo application

To understand how Global-OT module works and organizes data, you can run Global-OT demo application. It provides you with 
ability to work with repository which represents the history of counter modifications with the help of `Add` operation, which 
adds your modifications to commit graph.

As you might have already noticed, this approach resembles Git. The main advantage of Global-OT over Git is that there 
are no conflicts while synchronizing, which is conducted in background - you receive a single consistent result without 
any additional manual conflict resolution. Global-OT has already done it for you.

Also, demo application has an additional operation `New Manager` which adds new virtual participant to repository 
(will be opened in new browser tab).

There are two options for running this example: either with **Master Node** ([globally](#globally)), or without it 
([locally](#locally)).

### Locally
You can run the example in **5 steps**

#### 1. Clone DataKernel from GitHub repository and install it:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
```

#### 2. Install nodejs and npm on your computer (if not installed):
```
$ sudo apt install nodejs npm
$ sudo apt install npm
```
Please ensure that the version of installed nodejs is **8 or higher** by entering the following command:

```
$ nodejs --version
```

#### 3. Enter the following commands:
```
$ cd datakernel/examples/global-ot-demo/front
$ npm i
$ npm run-script build
```

Please note that these 3 steps need to be executed only once: you **don't** need to repeat them on every launch.

#### 4. Launch Demo application
```
$ cd datakernel/examples/global-ot-demo
$ mvn exec:java@GlobalOTDemoApp
```

#### 5. Open your favorite browser
Open your browser and go to [localhost:8080](http://localhost:8080). Now you can add modifications to the counter and add new users. 

Note, that by default when you click on "Add" button, there is a 1 second delay before modification will be pushed to repository. 
You can manage the durations of these delays and make them longer or remove them at all, which will make the app instant. 
To do so, you should start `GlobalOTDemoApp` with the following argument:
```
$ mvn exec:java@GlobalEditorLauncher -Dconfig.push.delay=PT0S
```
`PT0S` determines the delay, so in this case the delay is 0 seconds.

If you want to see forking and merging in edits history, make synchronization delay longer (for example, 20 seconds `PT20S`), and 
make some modifications for both client applications, and you'll see something like this:

<img src="http://datakernel.io/static/images/demo-history-graph.png">

Also, pay attention that this application can work offline. So if you make modifications while there is no connection, they will 
be successfully pushed when connection is restored.


### Globally
Another option is to start demo application globally, which means that your client applications will connect to **Master Node**. 
You can start the application globally in **7 steps**

#### 1. Clone DataKernel from GitHub repository and install it:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
```

#### 2. Install nodejs and npm on your computer (if not installed):
```
$ sudo apt install nodejs npm
$ sudo apt install npm
```
Please ensure that the version of installed nodejs is **8** or higher by entering the following command:

```
$ nodejs --version
```

Please note that these 2 steps need to be executed only once: you **don't** need to repeat them on every launch.

#### 3. Launch Master Node
```
$ cd datakernel/examples/global-examples-common
$ mvn exec:java@MasterNodeLauncher
```

#### 4. Enter the following commands:
```
$ cd datakernel/examples/global-ot-demo/front
$ npm i
$ npm run-script build
```

Step 4 also needs to be executed only once.

#### 5. Start the first Demo application:
```
$ cd datakernel/examples/global-ot-demo
$ mvn exec:java@GlobalOTDemoApp -Dconfig.discovery.masters=http://127.0.0.1:9000
```

#### 6. Start the second Demo application:
```
$ cd datakernel/examples/global-ot-demo
$ mvn exec:java@GlobalOTDemoApp -Dconfig.discovery.masters=http://127.0.0.1:9000 -Dconfig.http.listenAddresses=*:8081
```

#### 7. Open your favorite browser
Open your browser and go to [localhost:8080](http://localhost:8080) and [localhost:8081](http://localhost:8081). Just like 
in case of *Local* launching, now you can modify the counter and explore the history of modifications, which is now synchronized and 
stored on the *Master Node*.

All the features described for *Local* launch work in *Global* mode too.