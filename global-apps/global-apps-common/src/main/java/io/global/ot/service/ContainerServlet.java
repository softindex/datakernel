package io.global.ot.service;

import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.global.common.PubKey;
import org.jetbrains.annotations.NotNull;

public final class ContainerServlet implements AsyncServlet {
	private final AsyncServlet servlet;

	private ContainerServlet(ContainerManager<?> containerManager, AsyncServlet next) {
		servlet = RoutingServlet.create()
				.map("/:pubKey/*", request -> {
					PubKey pubKey;
					try {
						pubKey = PubKey.fromString(request.getPathParameter("pubKey"));
					} catch (ParseException ignored) {
						return Promise.of(HttpResponse.notFound404());
					}
					UserContainer container = containerManager.getUserContainer(pubKey);
					if (container == null) {
						return Promise.of(HttpResponse.notFound404());
					}
					request.attach(container);
					return next.serve(request);
				});
	}

	public static ContainerServlet create(ContainerManager<?> containerManager, AsyncServlet next) {
		return new ContainerServlet(containerManager, next);
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		return servlet.serve(request);
	}
}
