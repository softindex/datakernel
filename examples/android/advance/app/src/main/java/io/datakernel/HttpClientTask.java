package io.datakernel;

import android.content.Context;
import android.os.AsyncTask;
import android.widget.Toast;

import java.lang.ref.WeakReference;
import java.util.Arrays;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpMessage;
import io.datakernel.http.HttpRequest;

import static android.widget.Toast.LENGTH_SHORT;

public final class HttpClientTask extends AsyncTask<Void, String, Void> {
    private final WeakReference<Context> contextWeakReference;
    private final AsyncHttpClient client;
    private final Eventloop eventloop;

    private HttpClientTask(WeakReference<Context> contextWeakReference, AsyncHttpClient client, Eventloop eventloop) {
        this.contextWeakReference = contextWeakReference;
        this.client = client;
        this.eventloop = eventloop;
    }

    public static HttpClientTask create(Context context, AsyncHttpClient client, Eventloop eventloop) {
        eventloop.keepAlive(true);
        return new HttpClientTask(new WeakReference<>(context), client, eventloop);
    }

    @Override
    protected void onProgressUpdate(String... values) {
        super.onProgressUpdate(values);
        Context context = contextWeakReference.get();
        Toast.makeText(context, Arrays.toString(values), LENGTH_SHORT).show();
    }

    @Override
    protected Void doInBackground(Void... voids) {
        eventloop.run();
        return null;
    }

    public void sendRequest() {
        eventloop.execute(() -> client.request(HttpRequest.get("http://127.0.0.1:8080/"))
                .whenException(e -> publishProgress("Exception due to " + e.getMessage()))
                .then(HttpMessage::loadBody)
                .whenResult(body -> publishProgress("Result is " + body.toString())));
    }
}