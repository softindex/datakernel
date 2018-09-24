package io.global.globalfs.server.http;

import io.datakernel.async.Stage;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpHeaders;
import io.datakernel.http.HttpMethod;
import io.datakernel.http.HttpRequest;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.api.GlobalFsName;

import java.io.IOException;

public class RemoteDiscoveryService implements DiscoveryService {

	private final AsyncHttpClient client;
	private final String host;

	public RemoteDiscoveryService(AsyncHttpClient client, String host) {
		this.client = client;
		this.host = host;
	}

	@Override
	public Stage<SignedData<AnnounceData>> findServers(PubKey pubKey) {
		return client.request(HttpRequest.get("http://" + host + DiscoveryServlet.FIND + "?key=" + GlobalFsName.serializePubKey(pubKey)))
				.thenCompose(data -> {
					try {
						return Stage.of(SignedData.ofBytes(data.getBody().asArray(), AnnounceData::fromBytes));
					} catch (IOException e) {
						return Stage.ofException(e);
					}
				});
	}

	@Override
	public Stage<Void> announce(PubKey pubKey, SignedData<AnnounceData> announceData) {
		return client.request(HttpRequest.of(HttpMethod.PUT, "http://" + host + DiscoveryServlet.ANNOUNCE + "?key=" + GlobalFsName.serializePubKey(pubKey))
				.withBody(announceData.toBytes())
				.withHeader(HttpHeaders.HOST, host))
				.toVoid();
	}
}
