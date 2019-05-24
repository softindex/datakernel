package io.datakernel.http;

import io.datakernel.async.Promise;
import io.datakernel.exception.UncheckedException;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Executor;

public final class BlockingServletAdapter implements AsyncServlet {
	private final Executor executor;
	private final BlockingServlet rootServlet;

	private BlockingServletAdapter(Executor executor, BlockingServlet rootServlet) {
		this.executor = executor;
		this.rootServlet = rootServlet;
	}

	public static BlockingServletAdapter create(Executor executor, BlockingServlet servlet) {
		return new BlockingServletAdapter(executor, servlet);
	}

	@Override
	public @NotNull Promise<HttpResponse> serve(HttpRequest request) throws UncheckedException {
		return Promise.ofBlockingCallable(executor, () -> rootServlet.serve(request));
	}
}
