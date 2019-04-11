Global-OT editor application uses OT (operational transformation) algorithm to manage collaborative edits of several 
users in one document. 

There are two options for running this example: either with **Master Node** ([globally](#globally)), or without it 
([locally](#locally)).

### Locally
You can run the example in **5 steps**.

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
$ cd datakernel/examples/global-ot-editor/front
$ npm i
$ npm run-script build
```

Please note that these 3 steps need to be executed only once: you **don't** need to repeat them on every launch.

#### 4. Launch Editor application
```
$ cd datakernel/examples/global-ot-editor
$ mvn exec:java@GlobalEditorLauncher 
```

#### 5. Open your favorite browser
Open your browser and go to [localhost:8080](http://localhost:8080). Now you can start using the editor and see edits history 
represented as commit graph on the right. To create a new user, simply duplicate the tab. Now you can work with the document 
collaboratively.

Note, that by default there is a small delay between synchronizations, so if both users write edits while the delay, edits history 
will first have two local versions for each of the users. These versions will be automatically merged during the next synchronization.

You can manage the durations of these delays and make them longer or remove them at all, which will make the editor instant. 
To do so, you should start `GlobalEditorLauncher` with the following argument:
```
$ mvn exec:java@GlobalEditorLauncher -Dconfig.push.delay=PT0S
```
`PT0S` determines the delay, so in this case the delay is 0 seconds.

If you want to see forking and merging in edits history, make synchronization delay longer (for example, 20 seconds `PT20S`), 
make some edits from both of the started clients during the delay, and you'll see something like this:

<img src="http://datakernel.io/static/images/editor-history-graph.png">

Also, pay attention that this editor can work offline. So if you make edits while there is no connection, they will 
be successfully pushed when connection is restored.

### Globally
Another option is to start editor application globally, which means that your client applications will connect to **Master Node**. 
You can start the application globally in **7 steps**.

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
$ cd datakernel/examples/global-ot-editor/front
$ npm i
$ npm run-script build
```

Step 4 also needs to be executed only once.

#### 5. Start the first Editor application:
```
$ cd datakernel/examples/global-ot-editor
$ mvn exec:java@GlobalEditorLauncher -Dconfig.discovery.masters=http://127.0.0.1:9000
```

#### 6. Start the second Editor application:
```
$ cd datakernel/examples/global-ot-editor
$ mvn exec:java@GlobalEditorLauncher -Dconfig.discovery.masters=http://127.0.0.1:9000 -Dconfig.http.listenAddresses=*:8081
```

#### 7. Open your favorite browser
Open your browser and go to [localhost:8080](http://localhost:8080) and [localhost:8081](http://localhost:8081). Just like 
in case of *Local* launching, now you can collaboratively edit the document and explore the history of edits, which is now 
synchronized and stored on the **Master Node**.

All the features described for *Local* launch work in *Global* mode too.