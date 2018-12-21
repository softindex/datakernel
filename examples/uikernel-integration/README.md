## UIKernel Integration Example
In this example you will see an integration of UIKernel.io frontend JS library in DataKernel modules, such as
* Eventloop
* HTTP
* Boot

To run the example you should first clone the project from GitHub repository:
```
$ git clone https://github.com/softindex/datakernel.git
```
Then install npm:
```
$ sudo apt install npm
```
Now go to uikernel-integration directory and enter these commands:
```
$ cd datakernel/examples/uikernel-integration
$ sudo npm i
$ npm run-script build
```
If the commands won't work try also to enter this command after ```sudo npm i```:
```
$ npm run-script postintall 
```

Now open your browser and go to `localhost:8080`. You will see an editable users grid table with some pre-defined 
information. This grid supports searching by name, age and gender as well as ability to add new people.