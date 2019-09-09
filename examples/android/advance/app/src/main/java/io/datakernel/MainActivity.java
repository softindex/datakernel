package io.datakernel;

import android.content.Context;
import android.os.Bundle;

import androidx.appcompat.app.AppCompatActivity;

import io.datakernel.di.core.Injector;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.promise.Promise;

import static io.datakernel.http.HttpResponse.ok200;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class MainActivity extends AppCompatActivity {
    static {
        System.setProperty("ByteBufPool.minSize", "1");
        System.setProperty("ByteBufPool.maxSize", "1");
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        Injector injector = Injector.of(
                Module.create()
                        .bind(Context.class).toInstance(MainActivity.this)
                        .bind(Eventloop.class).toInstance(Eventloop.create())
                        .bind(AsyncHttpServer.class).to(AsyncHttpServer::create, Eventloop.class, AsyncServlet.class)
                        .bind(AsyncServlet.class).toInstance(request -> Promise.of(ok200().withPlainText("Hello World!")))
                        .bind(HttpServerTask.class).to(HttpServerTask::create, Context.class, AsyncHttpServer.class, Eventloop.class).export(),
                Module.create()
                        .bind(Eventloop.class).to(Eventloop::create)
                        .bind(Context.class).toInstance(MainActivity.this)
                        .bind(AsyncHttpClient.class).to(AsyncHttpClient::create, Eventloop.class)
                        .bind(HttpClientTask.class).to(HttpClientTask::create, Context.class, AsyncHttpClient.class, Eventloop.class).export());

        HttpServerTask serverTask = injector.getInstance(HttpServerTask.class);
        HttpClientTask clientTask = injector.getInstance(HttpClientTask.class);
        serverTask.executeOnExecutor(newSingleThreadExecutor());
        clientTask.executeOnExecutor(newSingleThreadExecutor());

        findViewById(R.id.sendRequest).setOnClickListener($ -> clientTask.sendRequest());
    }
}
