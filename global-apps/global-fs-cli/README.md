## Global-FS CLI

Global-FS CLI is a console Global-FS client. It allows to conduct basic
interactions with Global-FS nodes: upload and download files, delete them
and get a list of uploaded files.

To run the CLI, you should enter these commands in your console:

```
$ git clone --depth=1 https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
$ cd global-apps/global-fs-cli
$ mvn package
```
After that, file `target/global-fs-cli-VERSION-jar-with-dependencies.jar` is
the desired runnable. You can turn it into a command similarly to how it's done
below:

```
$ cp target/global-fs-cli-*-jar-with-dependencies.jar ~/.local/globalfs.jar
$ alias globalfs=`java -jar ~/.local/globalfs.jar'

$ globalfs
```

Now you can use Global-FS CLI, all of the commands are explained by the help
interface.
