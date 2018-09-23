package io.global.globalfs;

import io.datakernel.async.Stage;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.globalsync.util.SerializationUtils;

import java.io.IOException;

public class DiscoveryServlet {

	// why not?
	public static AsyncServlet wrap(DiscoveryService service) {
		return MiddlewareServlet.create()
				.with("/announce", req -> {
					try {
						// fixme - this is a prototype of a prototype
						return service.announce(null, SignedData.ofBytes(req.getBody().asArray(), AnnounceData::fromBytes))
								.thenApply($ -> HttpResponse.ok200());
					} catch (IOException e) {
						return Stage.ofException(e);
					}
				})
				.with("/find", req -> {
					try {
						return service.findServers(SerializationUtils.readPubKey(req.getBody()))
								.thenApply(data -> HttpResponse.ok200().withBody(data.toBytes()));
					} catch (IOException e) {
						return Stage.ofException(e);
					}
				});
	}
}
