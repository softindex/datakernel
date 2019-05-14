package io.global.common.discovery;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.http.ContentTypes.JSON_UTF_8;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;

public final class DiscoveryServiceDriverServlet implements AsyncServlet {
	static final StructuredCodec<AnnounceData> ANNOUNCE_DATA_CODEC = REGISTRY.get(AnnounceData.class);
	static final StructuredCodec<@Nullable AnnounceData> NULLABLE_ANNOUNCE_DATA_CODEC = REGISTRY.get(AnnounceData.class).nullable();

	private final RoutingServlet servlet;

	public DiscoveryServiceDriverServlet(DiscoveryServiceDriver driver) {
		servlet = servlet(driver);
	}

	private static RoutingServlet servlet(DiscoveryServiceDriver driver) {
		return RoutingServlet.create()
				.with(POST, "/announce", request -> {
					String key = request.getCookie("Key");
					if (key == null) {
						return Promise.ofException(new ParseException());
					}

					try {
						KeyPair keys = PrivKey.fromString(key).computeKeys();
						return request.getBody()
								.then(body -> {
									try {
										AnnounceData data = JsonUtils.fromJson(ANNOUNCE_DATA_CODEC, body.asString(UTF_8));
										return driver.announce(keys, data)
												.map($ -> HttpResponse.ok200());
									} catch (ParseException e) {
										return Promise.<HttpResponse>ofException(e);
									}
								});
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/find/:pubKey", request -> {
					String parameterPubKey = request.getPathParameter("pubKey");
					if (parameterPubKey == null) {
						return Promise.ofException(new ParseException());
					}
					try {
						PubKey pubKey = PubKey.fromString(parameterPubKey);
						return driver.find(pubKey)
								.map(data -> HttpResponse.ok200()
										.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(JSON_UTF_8))
										.withBody(JsonUtils.toJson(NULLABLE_ANNOUNCE_DATA_CODEC, data).getBytes(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/shareKey/:receiver", request -> {
					String parameterReceiver = request.getPathParameter("receiver");
					String key = request.getCookie("Key");
					if (parameterReceiver == null || key == null) {
						return Promise.ofException(new ParseException());
					}
					try {
						PubKey receiver = PubKey.fromString(parameterReceiver);
						PrivKey sender = PrivKey.fromString(key);
						return request.getBody()
								.then(body -> {
									try {
										return driver.shareKey(sender, receiver, SimKey.fromString(body.asString(UTF_8)))
												.map($ -> HttpResponse.ok200());
									} catch (ParseException e) {
										return Promise.<HttpResponse>ofException(e);
									}
								});
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/getSharedKey/:hash", request -> {
					String key = request.getCookie("Key");
					String parameterHash = request.getPathParameter("hash");
					if (key == null || parameterHash == null) {
						return Promise.ofException(new ParseException());
					}
					try {
						KeyPair keys = PrivKey.fromString(key).computeKeys();
						Hash hash = Hash.fromString(parameterHash);
						return driver.getSharedKey(keys, hash)
								.map(simKey -> {
									if (simKey == null) {
										return HttpResponse.ofCode(404);
									}
									return HttpResponse.ok200()
											.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(JSON_UTF_8))
											.withBody(('"' + simKey.asString() + '"').getBytes(UTF_8));
								});
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/getSharedKeys", request -> {
					String key = request.getCookie("Key");
					if (key == null) {
						return Promise.ofException(new ParseException());
					}
					try {
						KeyPair keys = PrivKey.fromString(key).computeKeys();
						return driver.getSharedKeys(keys)
								.map(simKeys -> HttpResponse.ok200()
										.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(JSON_UTF_8))
										.withBody((simKeys.stream()
												.map(simKey -> '"' + simKey.asString() + '"')
												.collect(joining(", ", "[", "]")))
												.getBytes(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		return servlet.serve(request);
	}
}
