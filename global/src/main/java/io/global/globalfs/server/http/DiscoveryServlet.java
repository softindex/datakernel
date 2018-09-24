package io.global.globalfs.server.http;

import io.datakernel.async.Stage;
import io.datakernel.http.*;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.api.GlobalFsName;

import java.io.IOException;

public class DiscoveryServlet {
	public static final String FIND = "/find";
	public static final String ANNOUNCE = "/announce";

	public static AsyncServlet wrap(DiscoveryService service) {
		return MiddlewareServlet.create()
				.with(HttpMethod.GET, FIND, req -> {
					PubKey pubKey = GlobalFsName.deserializePubKey(req.getQueryParameter("key"));
					if (pubKey == null) {
						return Stage.ofException(HttpException.badRequest400());
					}
					return service.findServers(pubKey)
							.thenCompose(data -> {
								if (data != null) {
									return Stage.of(HttpResponse.ok200().withBody(data.toBytes()));
								}
								return Stage.ofException(HttpException.notFound404());
							});
				})
				.with(HttpMethod.PUT, ANNOUNCE, req -> {
					PubKey pubKey = GlobalFsName.deserializePubKey(req.getQueryParameter("key"));
					if (pubKey == null) {
						return Stage.ofException(HttpException.badRequest400());
					}
					return req.getBodyStage()
							.thenCompose(body -> {
								try {
									return service.announce(pubKey, SignedData.ofBytes(body.getArray(), AnnounceData::fromBytes));
								} catch (IOException e) {
									return Stage.ofException(e);
								}
							})
							.thenApply($ -> HttpResponse.ok200());

				});
	}
}
