package io.datakernel;

import android.content.Context;
import android.os.AsyncTask;
import android.widget.Toast;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Arrays;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;

import static android.widget.Toast.LENGTH_SHORT;

public final class HttpServerTask extends AsyncTask<Void, String, Void> {
    private final WeakReference<Context> contextWeakReference;
    private final AsyncHttpServer server;
    private final Eventloop eventloop;

    private HttpServerTask(WeakReference<Context> contextWeakReference, AsyncHttpServer server, Eventloop eventloop) {
        this.contextWeakReference = contextWeakReference;
        this.eventloop = eventloop;
        this.server = server;
    }

    public static HttpServerTask create(Context context, AsyncHttpServer server, Eventloop eventloop) {
        return new HttpServerTask(new WeakReference<>(context), server.withListenPort(8080), eventloop);
    }

    @Override
    protected void onProgressUpdate(String... values) {
        super.onProgressUpdate(values);
        Context context = contextWeakReference.get();
        if (context != null) {
            Toast.makeText(context, Arrays.toString(values), LENGTH_SHORT).show();
        }
    }

    @Override
    protected Void doInBackground(Void... voids) {
        try {
            server.listen();
            publishProgress("Server running");
            eventloop.run();
        } catch (IOException e) {
            publishProgress("Exception due to " + e.getMessage());
        }
        return null;
    }
}