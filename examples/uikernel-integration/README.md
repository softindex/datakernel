[UIKernel Integration Example](https://github.com/softindex/datakernel/tree/master/examples/uikernel-integration/src/main/java/io/datakernel/examples) -
 integration of UIKernel.io frontend JS library with DataKernel modules.

You can launch this example in **5 steps**:

#### 1. Clone DataKernel from GitHub repository and install it:
```
$ git clone https://github.com/softindex/datakernel.git
$ cd datakernel
$ mvn clean install -DskipTests
```

#### 2. Install npm:
```
$ sudo apt install npm
```

#### 3. Enter the following commands:
```
$ cd datakernel/examples/uikernel-integration
$ sudo npm i
$ npm run-script build
```
If the commands won't work, try to enter this command after `sudo npm i`:
```
$ npm run-script postinstall 
```

#### 4. Start `UIKernelWebAppLauncher` 
In console:
```
$ cd datakernel/examples/uikernel-integration
$ mvn exec:java@UIKernelWebAppLauncher 
```
In IDE: open cloned project in IDE. Next, set up default working directory of run configurations in your IDE, so that 
the example can work correctly. In accordance to DataKernel module structure, the working directory should be set to the 
module folder. In IntelliJ IDEA you can do it in the following way: `Run -> Edit configurations -> 
|Run/Debug Configurations -> |Templates -> Application| -> |Working directory -> $MODULE_WORKING_DIR$||`.

Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA). Then open `UIKernelWebAppLauncher` 
class, which is located at **datakernel -> examples -> uikernel-integration** and run its *main()* method.

#### 5. Open your favourite browser
Open your browser and go to [localhost:8080](http://localhost:8080). You will see an editable users grid table with 
some pre-defined information. This grid supports searching by name, age and gender. You can also add new people.