package io.datakernel;

import android.content.Context;
import android.os.AsyncTask;
import android.util.Log;
import android.widget.Toast;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Arrays;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.promise.Promise;

import static io.datakernel.http.HttpResponse.ok200;

public final class HttpServerTask extends AsyncTask<Void, String, Void> {
        private final Eventloop eventloop = Eventloop.create();
        private final WeakReference<Context> weakReferenceContext;
        private final AsyncHttpServer httpServer = AsyncHttpServer.create(eventloop,
                request -> Promise.of(ok200().withPlainText("Hello World!")))
                .withListenPort(8080);

        private HttpServerTask(WeakReference<Context> weakReferenceContext) {
            this.weakReferenceContext = weakReferenceContext;
        }

        public static HttpServerTask create(Context context) {
            return new HttpServerTask(new WeakReference<>(context));
        }

        @Override
        protected void onProgressUpdate(String... values) {
            Context context = weakReferenceContext.get();
            if (context != null) {
                Toast.makeText(context, Arrays.toString(values), Toast.LENGTH_SHORT).show();
            } else {
                Log.e(HttpServerTask.class.getName(), "Context has been recycled");
            }
        }

        @Override
        protected Void doInBackground(Void... voids) {
            eventloop.withCurrentThread();
            try {
                httpServer.listen();
                eventloop.run();
            } catch (IOException e) {
                publishProgress("Cannot run server due to " + e.getMessage());
            }
            return null;
        }
    }