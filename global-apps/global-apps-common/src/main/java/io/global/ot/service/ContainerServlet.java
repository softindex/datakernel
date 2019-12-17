package io.global.ot.service;

import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.promise.Async;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.http.HttpHeaders.HOST;

public final class ContainerServlet implements AsyncServlet {
	private final ContainerManager<?> containerManager;
	private final AsyncServlet next;

	@Nullable
	private UserContainer singleContainer;

	private ContainerServlet(ContainerManager<?> containerManager, AsyncServlet next, @Nullable UserContainer singleContainer) {
		this.containerManager = containerManager;
		this.next = next;
		this.singleContainer = singleContainer;
	}

	public static ContainerServlet create(ContainerManager<?> containerManager, AsyncServlet next) {
		return new ContainerServlet(containerManager, next,
				containerManager.isSingleMode() ?
						containerManager.getUserContainer("") :
						null);
	}

	@NotNull
	@Override
	public Async<HttpResponse> serve(@NotNull HttpRequest request) {
		if (singleContainer != null) {
			request.attach(singleContainer);
			request.attach(UserContainer.class, singleContainer);
			return next.serve(request);
		}
		String header = request.getHeader(HOST);
		if (header == null) {
			return Promise.of(HttpResponse.ofCode(400).withPlainText("HTTP Header 'Host' is required"));
		}
		String[] parts = header.split("\\.", 2);
		if (parts.length != 2) {
			return Promise.of(HttpResponse.ofCode(400).withPlainText("Could not find container id in a subdomain"));
		}
		UserContainer container = containerManager.getUserContainer(parts[0]);
		if (container == null) {
			return Promise.of(HttpResponse.notFound404().withPlainText("No container is running for a given id"));
		}
		request.attach(container);
		request.attach(UserContainer.class, container);
		return next.serve(request);
	}
}
