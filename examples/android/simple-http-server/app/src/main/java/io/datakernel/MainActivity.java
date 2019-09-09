package io.datakernel;

import android.os.Bundle;

import androidx.appcompat.app.AppCompatActivity;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class MainActivity extends AppCompatActivity {
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        HttpServerTask.create(this).executeOnExecutor(newSingleThreadExecutor());
        HttpClientTask client = HttpClientTask.create(this);
        client.executeOnExecutor(newSingleThreadExecutor());

        findViewById(R.id.sendRequest).setOnClickListener($ -> client.sendRequest());
    }
}
