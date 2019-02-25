## Global-FS CLI

Global-FS-CLI is a command-line interface of Global-FS client application. It allows to conduct basic interactions with 
Global-FS nodes: upload and download files, delete them and get list of uploaded files.

To run the CLI, you should enter these commands in your console in appropriate folder:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd examples/global-fs-cli
$ mvn package
$ alias globalfs=`java -jar $1'
$ # where $1 is the name of compiled .jar file
$ globalfs
```
Now you can use Global-FS CLI, all of the commands are explained in the interface. Connect to any Global-FS node and 
utilize your private key. Pay attention that there are several useful options for file upload and download configurations.
