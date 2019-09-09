package io.datakernel;

import android.content.Context;

import dagger.Module;
import dagger.Provides;

@Module
public final class AppModule {
    private final Context context;

    AppModule(Context context) {
        this.context = context;
    }

    @Provides
    Context context() {
        return context;
    }
}