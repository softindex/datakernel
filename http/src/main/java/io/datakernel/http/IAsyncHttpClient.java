package io.datakernel.http;

import java.util.concurrent.CompletionStage;

public interface IAsyncHttpClient {
	CompletionStage<HttpResponse> send(HttpRequest request);
}
