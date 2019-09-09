package io.datakernel;

import android.content.Context;

import javax.inject.Singleton;

import dagger.Component;

@Singleton
@Component(modules = {AppModule.class, HttpClientModule.class, HttpServerModule.class})
public interface AppComponent {
    void inject(Context context);

    HttpClientTask client();

    HttpServerTask server();
}