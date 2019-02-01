package io.global.fs.demo;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.loader.StaticLoader;
import io.datakernel.util.Tuple2;
import io.global.common.*;
import io.global.fs.http.GlobalFsDriverServlet;
import io.global.fs.local.GlobalFsDriver;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class ServletModule extends AbstractModule {
	private static final StructuredCodec<PubKey> PUB_KEY_CODEC = STRING_CODEC.transform(PubKey::fromString, PubKey::asString);
	private static final StructuredCodec<PrivKey> PRIV_KEY_CODEC = STRING_CODEC.transform(PrivKey::fromString, PrivKey::asString);
	private static final StructuredCodec<SimKey> SIM_KEY_CODEC = STRING_CODEC.transform(SimKey::fromString, SimKey::asString);

	private static final StructuredCodec<Hash> HASH_CODEC = STRING_CODEC.transform(Hash::fromString, Hash::asString);

	private static final StructuredCodec<KeyPair> KEY_PAIR_CODEC =
			tuple(KeyPair::new,
					KeyPair::getPrivKey, PRIV_KEY_CODEC,
					KeyPair::getPubKey, PUB_KEY_CODEC);

	private static final StructuredCodec<Tuple2<SimKey, Hash>> SIM_KEY_AND_HASH_CODEC =
			tuple(Tuple2::new,
					Tuple2::getValue1, SIM_KEY_CODEC,
					Tuple2::getValue2, HASH_CODEC);

	private static final HttpHeaderValue CONTENT_TYPE_JSON = HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.JSON));

	@Provides
	@Singleton
	AsyncServlet provide(Eventloop eventloop, GlobalFsDriver driver, StaticLoader resourceLoader) {
		return MiddlewareServlet.create()
				.with("", new GlobalFsDriverServlet(driver))
				.with(GET, "/", SingleResourceStaticServlet.create(eventloop, resourceLoader, "index.html"))
				.with(GET, "/view", SingleResourceStaticServlet.create(eventloop, resourceLoader, "key-view.html"))
				.with(GET, "/genKeyPair", request -> {
					KeyPair pair = KeyPair.generate();
					return Promise.of(HttpResponse.ok200()
							.withHeader(CONTENT_TYPE, CONTENT_TYPE_JSON)
							.withBody(JsonUtils.toJson(KEY_PAIR_CODEC, pair).getBytes(UTF_8)));
				})
				.with(GET, "/genSimKey", request -> {
					SimKey simKey = SimKey.generate();
					Hash hash = Hash.sha1(simKey.getBytes());
					return Promise.of(HttpResponse.ok200()
							.withBody(JsonUtils.toJson(SIM_KEY_AND_HASH_CODEC, new Tuple2<>(simKey, hash)).getBytes(UTF_8))
							.withHeader(CONTENT_TYPE, CONTENT_TYPE_JSON));
				})
				.withFallback(StaticServlet.create(eventloop, resourceLoader, "404.html"));
	}
}
