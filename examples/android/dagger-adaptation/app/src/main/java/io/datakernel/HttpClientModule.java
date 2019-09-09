package io.datakernel;

import javax.inject.Named;
import javax.inject.Singleton;

import dagger.Module;
import dagger.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;

@Module
public class HttpClientModule {
    @Provides
    @Singleton
    static AsyncHttpClient provideMyExample(@Named("client") Eventloop eventloop) {
        return AsyncHttpClient.create(eventloop);
    }

    @Named("client")
    @Provides
    @Singleton
    static Eventloop eventloop() {
        Eventloop eventloop = Eventloop.create();
        eventloop.keepAlive(true);
        return eventloop;
    }
}