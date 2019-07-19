## Global Files application server

To launch the application, you should run these two commands (assuming you have [Docker installed](https://docs.docker.com/install/)) :
```bash
$ docker build . -t global-files
$ docker run --rm -p8080:8080 global-files
```
