package io.datakernel.http;

import io.datakernel.async.Callback;
import io.datakernel.async.Stage;

public abstract class BlockingServlet implements AsyncServlet {
	@Override
	public final void serve(HttpRequest request, Callback<HttpResponse> callback) {
		try {
			HttpResponse httpResponse = serveBlocking(request);
			callback.set(httpResponse);
		} catch (Exception e) {
			callback.setException(e);
		}
	}

	@Override
	public final Stage<HttpResponse> serve(HttpRequest request) {
		throw new UnsupportedOperationException();
	}

	abstract public HttpResponse serveBlocking(HttpRequest request) throws Exception;
}
