package io.datakernel.http;

import io.datakernel.async.ResultCallback;

import java.util.concurrent.CompletionStage;

public abstract class BlockingServlet implements AsyncServlet {
	@Override
	public final void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
		try {
			HttpResponse httpResponse = serveBlocking(request);
			callback.set(httpResponse);
		} catch (Exception e) {
			callback.setException(e);
		}
	}

	@Override
	public final CompletionStage<HttpResponse> serve(HttpRequest request) {
		throw new UnsupportedOperationException();
	}

	abstract public HttpResponse serveBlocking(HttpRequest request) throws Exception;
}
