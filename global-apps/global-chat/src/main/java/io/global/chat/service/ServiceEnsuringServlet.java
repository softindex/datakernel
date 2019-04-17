package io.global.chat.service;

import io.datakernel.async.AsyncPredicate;
import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.global.common.PrivKey;
import org.jetbrains.annotations.NotNull;

public final class ServiceEnsuringServlet implements AsyncServlet {
	private final RoomListServiceHolder serviceHolder;
	private final AsyncServlet next;

	private AsyncPredicate<PrivKey> keyValidator = AsyncPredicate.alwaysTrue();

	private ServiceEnsuringServlet(RoomListServiceHolder serviceHolder, AsyncServlet next) {
		this.serviceHolder = serviceHolder;
		this.next = next;
	}

	public static ServiceEnsuringServlet create(RoomListServiceHolder serviceHolder, AsyncServlet next) {
		return new ServiceEnsuringServlet(serviceHolder, next);
	}

	public ServiceEnsuringServlet withKeyValidator(AsyncPredicate<PrivKey> keyValidator) {
		this.keyValidator = keyValidator;
		return this;
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(HttpRequest request) throws UncheckedException {
		try {
			PrivKey privKey = PrivKey.fromString(request.getCookie("Key"));
			return keyValidator.test(privKey)
					.whenResult(isValid -> {
						if (isValid) serviceHolder.ensureRoomListService(privKey);
					})
					.then($ -> next.serve(request));
		} catch (ParseException e) {
			return Promise.ofException(e);
		}
	}
}
