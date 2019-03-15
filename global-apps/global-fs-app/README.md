## Global-FS application server

To launch the application, you should run these two commands (assuming you have Docker installed) :
```bash
$ docker build . -t global-fs-app
$ docker run --rm -p8080:8080 global-fs-app
```
