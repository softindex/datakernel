package io.global.fs.app.container;

import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.global.fs.app.container.Utils.PARTICIPANTS_CODEC;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class FsMessagingServlet implements AsyncServlet {
	private final FsUserContainerHolder containerHolder;

	private FsMessagingServlet(FsUserContainerHolder containerHolder) {
		this.containerHolder = containerHolder;
	}

	public static FsMessagingServlet create(FsUserContainerHolder containerHolder) {
		return new FsMessagingServlet(containerHolder);
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(HttpRequest request) throws UncheckedException {
		return request.getBody()
				.then(body -> {
					try {
						PrivKey privKey = PrivKey.fromString(request.getCookie("Key"));
						String dirName = request.getPathParameter("dirName");
						Set<PubKey> participants = fromJson(PARTICIPANTS_CODEC, body.getString(UTF_8));
						return containerHolder.ensureUserContainer(privKey)
								.then(container -> container.getMessagingService().sendMessage(dirName, participants))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					} finally {
						body.recycle();
					}

				});
	}
}
