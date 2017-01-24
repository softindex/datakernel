package io.datakernel.http;

import io.datakernel.async.ResultCallback;

public interface IAsyncHttpClient {
	void send(HttpRequest request, ResultCallback<HttpResponse> callback);
}
