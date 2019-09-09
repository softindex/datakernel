package io.datakernel;

import android.app.Application;
import android.content.Context;

public class App extends Application {
        private AppComponent component;

        @Override
        public void onCreate() {
            super.onCreate();
            component = DaggerAppComponent.builder()
                    .appModule(new AppModule(this))
                    .build();
        }

        public static AppComponent component(Context context) {
            return ((App) context.getApplicationContext()).component;
        }
    }