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
					try {
						KeyPair keys = PrivKey.fromString(request.getCookie("Key")).computeKeys();
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
					try {
						PubKey pubKey = PubKey.fromString(request.getPathParameter("pubKey"));
						return driver.find(pubKey)
								.map(data -> HttpResponse.ok200()
										.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(JSON_UTF_8))
										.withBody(JsonUtils.toJson(NULLABLE_ANNOUNCE_DATA_CODEC, data).getBytes(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/shareKey/:receiver", request -> {
					try {
						PubKey receiver = PubKey.fromString(request.getPathParameter("receiver"));
						PrivKey sender = PrivKey.fromString(request.getCookie("Key"));
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
					try {
						KeyPair keys = PrivKey.fromString(request.getCookie("Key")).computeKeys();
						Hash hash = Hash.fromString(request.getPathParameter("hash"));
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
					try {
						KeyPair keys = PrivKey.fromString(request.getCookie("Key")).computeKeys();
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
