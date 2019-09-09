package io.datakernel;

import android.content.Context;
import android.os.AsyncTask;
import android.widget.Toast;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Arrays;

import javax.inject.Inject;
import javax.inject.Named;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;

public final class HttpServerTask extends AsyncTask<Void, String, Void> {
        private final Eventloop eventloop;
        private final WeakReference<Context> contextReference;
        private final AsyncHttpServer server;

        @Inject
        HttpServerTask(@Named("server") Eventloop eventloop, Context contextReference, AsyncHttpServer server) {
            this.contextReference = new WeakReference<>(contextReference);
            this.eventloop = eventloop;
            this.server = server;
        }

        @Override
        protected void onProgressUpdate(String... values) {
            super.onProgressUpdate(values);
            Context context = contextReference.get();
            if (context != null) {
                Toast.makeText(context, Arrays.toString(values), Toast.LENGTH_SHORT).show();
            }
        }

        @Override
        protected Void doInBackground(Void... voids) {
            try {
                server.listen();
            } catch (IOException e) {
                publishProgress("Error happened - " + e.getMessage());
            }
            publishProgress("Running server");
            eventloop.withCurrentThread().run();
            return null;
        }
    }