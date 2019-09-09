package io.datakernel;

import android.content.Context;
import android.os.AsyncTask;
import android.util.Log;
import android.widget.Toast;

import java.lang.ref.WeakReference;
import java.util.Arrays;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpMessage;
import io.datakernel.http.HttpRequest;

public final class HttpClientTask extends AsyncTask<Void, String, Void> {
        private final Eventloop eventloop = Eventloop.create();
        private final WeakReference<Context> weakReferenceContext;
        private final AsyncHttpClient client = AsyncHttpClient.create(eventloop);

        private HttpClientTask(WeakReference<Context> weakReferenceContext) {
            this.weakReferenceContext = weakReferenceContext;
            eventloop.keepAlive(true);
        }

        public static HttpClientTask create(Context context) {
            return new HttpClientTask(new WeakReference<>(context));
        }

        @Override
        protected void onProgressUpdate(String... values) {
            Context context = weakReferenceContext.get();
            if (context != null) {
                Toast.makeText(context, Arrays.toString(values), Toast.LENGTH_SHORT).show();
            } else {
                Log.e(HttpClientTask.class.getName(), "Context has been recycled");
            }
        }

        @Override
        protected Void doInBackground(Void... voids) {
            eventloop.withCurrentThread();
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