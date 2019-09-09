package io.datakernel;

import javax.inject.Named;
import javax.inject.Singleton;

import dagger.Module;
import dagger.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.promise.Promise;

import static io.datakernel.http.HttpResponse.ok200;

@Module
public final class HttpServerModule {
    @Provides
    @Singleton
    static AsyncHttpServer asyncHttpServer(@Named("server") Eventloop eventloop) {
        return AsyncHttpServer.create(eventloop,
                $ -> Promise.of(ok200().withPlainText("Hello World!")))
                .withListenPort(8080);
    }

    @Named("server")
    @Provides
    @Singleton
    static Eventloop eventloop() {
        Eventloop eventloop = Eventloop.create();
        eventloop.keepAlive(true);
        return eventloop;
    }
}