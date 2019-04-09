Global-OT Chat application uses OT (operational transformation) algorithm to manage messages history of several users, 
particularly in case of bad network scenarios, which is simulated with the help of delays. 

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
$ cd datakernel/examples/global-ot-chat/front
$ npm i
$ npm run-script build
```

Please note that these 3 steps need to be executed only once: you **don't** need to repeat them on every launch.

#### 4. Launch Chat application
```
$ cd datakernel/examples/global-ot-chat
$ mvn exec:java@GlobalChatDemoApp 
```

#### 5. Open your favorite browser
Open your browser and go to [localhost:8080](http://localhost:8080). First, you will be asked to enter *Login* (you can enter 
whichever you wish). After that, you can start using the chat, send messages and see their history represented as commit graph 
by clicking on the top right graph icon. 

<img src="http://datakernel.io/static/images/ot-chat-hint.png">

<br>

To create a new user, simply duplicate the tab and enter *Login* for the second user. Now you can have a conversation. Note, 
that by default there is a delay between synchronizations, so if both users write messages while the delay, message history 
will first have two local versions for each of the users and these versions will be merged during the next synchronization.

You can manage the durations of these delays and make them longer or remove them at all, which will make the chat instant. 
To do so, you should start `GlobalChatDemoApp` with the following argument:
```
$ mvn exec:java@GlobalChatDemoApp -Dconfig.push.delay=PT0S
```
`PT0S` determines the delay, so in this case the delay is 0 seconds.

Or you can create a `client.properties` file in **global-ot-chat** folder and enter the following configuration:
```
push.delay=0 seconds
```

If you want to see forking and merging of chat history, make synchronization delay longer (for example, 20 seconds `PT20S`), 
write messages from both of the started clients during the delay, and you'll see something like this:

<img src="http://datakernel.io/static/images/chat-history-graph.png">

Also, pay attention that this chat can work offline. So if you send messages while there is no connection, the messages will 
be successfully pushed when connection is restored.

### Globally
Another option is to start chat application globally, which means that your client applications will connect to **Master Node**. 
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
$ cd datakernel/examples/global-ot-chat/front
$ npm i
$ npm run-script build
```

Step 4 also needs to be executed only once.

#### 5. Start the first Chat application:
```
$ cd datakernel/examples/global-ot-chat
$ mvn exec:java@GlobalChatDemoApp -Dconfig.discovery.masters=http://127.0.0.1:9000
```

#### 6. Start the second Chat application:
```
$ cd datakernel/examples/global-ot-chat
$ mvn exec:java@GlobalChatDemoApp -Dconfig.discovery.masters=http://127.0.0.1:9000 -Dconfig.http.listenAddresses=*:8081
```

#### 7. Open your favorite browser
Open your browser and go to [localhost:8080](http://localhost:8080) and [localhost:8081](http://localhost:8081). Just like 
in case of *Local* launching, you should also enter any *Logins* for both of the launched applications. Now you can send messages 
and explore their history, which is now synchronized and stored on the *Master Node*.

All the features described for *Local* launch work in *Global* mode too.