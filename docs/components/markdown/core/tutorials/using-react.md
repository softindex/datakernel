---
id: using-react
filename: tutorials/using-react
title: Using React in DataKernel projects
nav-menu: core
layout: core
prev: core/tutorials/authorization-tutorial.html
next: core/tutorials/todo-list-tutorial.html
---
## Introduction
In this example you can explore how to integrate React in DataKernel projects. **You can find full example sources on 
[GitHub](https://github.com/softindex/datakernel/blob/master/examples/tutorials/advanced-react-integration)**.

Here we will consider DataKernel **HttpServerLauncher** and **AsyncServlet** which are used to set up the 
[server](#creating-launcher), which processes the requests. See how DataKernel makes this process extremely simple.

### Creating launcher
**SimpleApplicationLauncher** extends DataKernel **HttpServerLauncher**:
{% highlight java %}
{% github_sample softindex/datakernel/blob/master/examples/tutorials/react-integration/src/main/java/SimpleApplicationLauncher.java tag:EXAMPLE %}
{% endhighlight %}

**HttpServerLauncher** takes care of setting up all the needed configurations for HTTP server.

Provide **AsyncServlet**, which will open the **index.html** of the provided path.

Then write down *main* method, which will launch **SimpleApplicationLauncher**.
And that's it, no additional configurations are required.

## Running the application
If you want to run the example, [Clone DataKernel](https://github.com/softindex/datakernel.git) and import it 
as a Maven project. Before running the example, build the project (**Ctrl + F9** for IntelliJ IDEA).

Run the following command in example's folder in terminal:
{% highlight bash %}
$ npm run-script build
{% endhighlight %}

Open `SimpleApplicationLauncher` class and run its *main()* method.
Then open your favourite browser and go to [localhost:8080](http://localhost:8080).
