package io.datakernel;

import android.content.Context;
import android.os.AsyncTask;
import android.widget.Toast;

import java.lang.ref.WeakReference;
import java.util.Arrays;

import javax.inject.Inject;
import javax.inject.Named;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpMessage;
import io.datakernel.http.HttpRequest;

public final class HttpClientTask extends AsyncTask<Void, String, Void> {
        private final WeakReference<Context> contextReference;
        private final AsyncHttpClient client;
        private final Eventloop eventloop;

        @Inject
        HttpClientTask(Context context, @Named("client") Eventloop eventloop, AsyncHttpClient client) {
            this.contextReference = new WeakReference<>(context);
            this.eventloop = eventloop;
            this.client = client;
        }

        @Override
        protected void onProgressUpdate(String... values) {
            super.onProgressUpdate(values);
            Context context = contextReference.get();
            if (context != null)
                Toast.makeText(context, Arrays.toString(values), Toast.LENGTH_SHORT).show();
        }

        @Override
        protected Void doInBackground(Void... voids) {
            eventloop.withCurrentThread().run();
            return null;
        }

        void sendRequest() {
            eventloop.post(() -> {
                client.request(HttpRequest.get("http://127.0.0.1:8080/"))
                        .whenException(e -> publishProgress("Cannot send message, cause - " + e.getMessage()))
                        .then(HttpMessage::loadBody)
                        .whenResult(body -> publishProgress("Message: " + body));
            });
        }
    }