package io.global.ot.service;

import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.global.common.PrivKey;
import org.jetbrains.annotations.NotNull;

public final class ServiceEnsuringServlet implements AsyncServlet {
	private final UserContainerHolder<?> serviceHolder;
	private final AsyncServlet next;

	private ServiceEnsuringServlet(UserContainerHolder serviceHolder, AsyncServlet next) {
		this.serviceHolder = serviceHolder;
		this.next = next;
	}

	public static ServiceEnsuringServlet create(UserContainerHolder serviceHolder, AsyncServlet next) {
		return new ServiceEnsuringServlet(serviceHolder, next);
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(HttpRequest request) throws UncheckedException {
		try {
			PrivKey privKey = PrivKey.fromString(request.getCookie("Key"));
			return serviceHolder.ensureUserContainer(privKey)
					.then($ -> next.serve(request));
		} catch (ParseException e) {
			return Promise.ofException(e);
		}
	}
}
