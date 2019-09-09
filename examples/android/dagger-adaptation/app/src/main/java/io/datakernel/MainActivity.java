package io.datakernel;

import android.os.Bundle;

import androidx.appcompat.app.AppCompatActivity;

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
        AppComponent component = App.component(this);
        component.inject(this);
        HttpClientTask client = component.client();
        client.executeOnExecutor(newSingleThreadExecutor());
        component.server().executeOnExecutor(newSingleThreadExecutor());

        findViewById(R.id.makeRequest).setOnClickListener($ -> client.sendRequest());
    }
}
