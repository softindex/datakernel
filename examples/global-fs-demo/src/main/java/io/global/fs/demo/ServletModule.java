package io.global.fs.demo;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.*;
import io.datakernel.loader.StaticLoader;
import io.global.common.*;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.demo.RepoManager.Repo;
import io.global.fs.http.RemoteFsServlet;
import io.global.fs.local.GlobalFsAdapter;
import io.global.fs.local.GlobalFsDriver;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class ServletModule extends AbstractModule {
	private static final StructuredCodec<PubKey> PUB_KEY_CODEC = STRING_CODEC.transform(PubKey::fromString, PubKey::asString);
	private static final StructuredCodec<PrivKey> PRIV_KEY_CODEC = STRING_CODEC.transform(PrivKey::fromString, PrivKey::asString);

	private static final StructuredCodec<Set<PubKey>> PUB_KEYS_CODEC = ofSet(PUB_KEY_CODEC);
	private static final StructuredCodec<Hash> HASH_CODEC = REGISTRY.get(Hash.class);
	private static final StructuredCodec<Set<Hash>> SIM_KEYS_CODEC = ofSet(HASH_CODEC);

	private static final StructuredCodec<KeyPair> KEY_PAIR_CODEC =
			tuple(KeyPair::new,
					KeyPair::getPrivKey, PRIV_KEY_CODEC,
					KeyPair::getPubKey, PUB_KEY_CODEC);

	private static final StructuredCodec<List<GlobalFsCheckpoint>> EXTENDED_LIST_CODEC =
			ofList(tuple(GlobalFsCheckpoint::of,
					GlobalFsCheckpoint::getFilename, STRING_CODEC,
					GlobalFsCheckpoint::getPosition, LONG_CODEC,
					GlobalFsCheckpoint::getDigest, (STRING_CODEC.transform($ -> {
						throw new UnsupportedOperationException();
					}, digest -> {
						if (digest == null) {
							return null;
						}
						byte[] hash = new byte[digest.getDigestSize()];
						new SHA256Digest(digest).doFinal(hash, 0);
						return CryptoUtils.toHexString(hash);
					})),
					GlobalFsCheckpoint::getSimKeyHash, REGISTRY.get(Hash.class).nullable()));

	private static final HttpHeaderValue CONTENT_TYPE_JSON = HttpHeaderValue.ofContentType(ContentType.of(MediaTypes.JSON));

	@Provides
	@Singleton
	AsyncServlet provide(Eventloop eventloop, GlobalFsDriver driver, RepoManager repoManager, StaticLoader resourceLoader) {
		return MiddlewareServlet.create()
				.with(GET, "/", SingleResourceStaticServlet.create(eventloop, resourceLoader, "index.html"))
				.with(GET, "/newRepo", request -> {
					KeyPair pair = repoManager.newRepo();
					return Promise.of(HttpResponse.ok200()
							.withHeader(CONTENT_TYPE, CONTENT_TYPE_JSON)
							.withBody(JsonUtils.toJson(KEY_PAIR_CODEC, pair).getBytes(UTF_8)));
				})
				.with(GET, "/addRepo/:privKey", request -> {
					try {
						PubKey pubKey = repoManager.addRepo(PrivKey.fromString(request.getPathParameter("privKey")));
						return Promise.of(HttpResponse.ok200()
								.withHeader(CONTENT_TYPE, CONTENT_TYPE_JSON)
								.withBody(JsonUtils.toJson(PUB_KEY_CODEC, pubKey).getBytes(UTF_8)));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/listRepos", request ->
						Promise.of(HttpResponse.ok200()
								.withHeader(CONTENT_TYPE, CONTENT_TYPE_JSON)
								.withBody(JsonUtils.toJson(PUB_KEYS_CODEC, repoManager.getRepos()).getBytes(UTF_8))))
				.with(GET, "/deleteRepo/:privKey", request -> {
					try {
						PrivKey privKey = PrivKey.fromString(request.getPathParameter("privKey"));
						repoManager.delete(privKey);
						return Promise.of(HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with("/:owner", createPubKeyServlet(eventloop, resourceLoader, driver, repoManager))
				.with("", createSimKeyServlet(repoManager))
				.withFallback(StaticServlet.create(eventloop, resourceLoader));
	}

	private AsyncServlet createPubKeyServlet(Eventloop eventloop, StaticLoader resourceLoader, GlobalFsDriver driver, RepoManager repoManager) {
		Map<GlobalFsAdapter, RemoteFsServlet> servlets = new HashMap<>();
		return MiddlewareServlet.create()
				.with(GET, "/", SingleResourceStaticServlet.create(eventloop, resourceLoader, "key-view.html"))
				.with("/gateway", MiddlewareServlet.create()
						.withFallback(request -> {
							try {
								PubKey pubKey = PubKey.fromString(request.getPathParameter("owner"));
								String privKeyStr = request.getHeaderOrNull(HttpHeaders.of("Key"));
								PrivKey privKey = privKeyStr != null ? PrivKey.fromString(privKeyStr) : null;
								Repo repo = repoManager.getRepo(pubKey);
								if (repo == null) {
									throw HttpException.ofCode(404, "No such repo");
								}
								GlobalFsAdapter gateway;
								if (privKey != null) {
									if (!privKey.computePubKey().equals(pubKey)) {
										return Promise.ofException(HttpException.ofCode(400, "Given private key does not match requested public key"));
									}
									gateway = repo.getGateway();
								} else {
									gateway = repo.getPublicGateway();
								}
								return servlets.computeIfAbsent(gateway, RemoteFsServlet::create).serve(request);
							} catch (ParseException | HttpException e) {
								throw new UncheckedException(e);
							}
						}))
				.with(GET, "/list", request -> {
					try {
						PubKey pubKey = PubKey.fromString(request.getPathParameter("owner"));
						String glob = request.getQueryParameter("glob", "**");
						return driver.gatewayFor(pubKey).extentedList(glob)
								.thenApply(list -> HttpResponse.ok200()
										.withBody(JsonUtils.toJson(EXTENDED_LIST_CODEC, list).getBytes(UTF_8))
										.withHeader(CONTENT_TYPE, CONTENT_TYPE_JSON));
					} catch (ParseException e) {
						throw new UncheckedException(e);
					}
				});
	}

	private MiddlewareServlet createSimKeyServlet(RepoManager repoManager) {
		return MiddlewareServlet.create()
				.with(GET, "/listKeys", request -> {
					try {
						Repo repo = repoManager.getRepo(getPrivKey(request).computePubKey());
						if (repo == null) {
							throw HttpException.ofCode(404, "No such repo");
						}
						return Promise.of(HttpResponse.ok200()
								.withBody(JsonUtils.toJson(SIM_KEYS_CODEC, repo.listKeys()).getBytes(UTF_8))
								.withHeader(HttpHeaders.CONTENT_TYPE, CONTENT_TYPE_JSON));
					} catch (ParseException | HttpException e) {
						return Promise.ofException(e);
					}
				})
				.with(GET, "/newKey", request -> {
					try {
						Repo repo = repoManager.getRepo(getPrivKey(request).computePubKey());
						if (repo == null) {
							throw HttpException.ofCode(404, "No such repo");
						}
						return Promise.of(HttpResponse.ok200()
								.withBody(JsonUtils.toJson(HASH_CODEC, repo.newKey()).getBytes(UTF_8))
								.withHeader(CONTENT_TYPE, CONTENT_TYPE_JSON));
					} catch (ParseException | HttpException e) {
						return Promise.ofException(e);
					}
				});
	}

	private PrivKey getPrivKey(HttpRequest request) throws ParseException, HttpException {
		String cookieStr = request.getHeaderOrNull(HttpHeaders.of("Key"));
		if (cookieStr == null) {
			throw HttpException.ofCode(400, "No private key cookie");
		}
		return PrivKey.fromString(cookieStr);
	}
}
