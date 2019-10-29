package io.global.ot;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.common.parse.ParseException;
import io.datakernel.http.*;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTUplink.FetchData;
import io.datakernel.ot.OTUplinkImpl;
import io.datakernel.promise.Async;
import io.datakernel.promise.Promise;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.ot.api.CommitId;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.client.OTRepositoryAdapter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.MediaTypes.JSON;
import static io.datakernel.http.MediaTypes.PLAIN_TEXT;
import static io.global.ot.api.OTUplinkCommand.*;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;

public final class DynamicOTUplinkServlet<D> implements AsyncServlet {
	private static final Logger logger = LoggerFactory.getLogger(DynamicOTUplinkServlet.class);

	public static final ParseException ID_REQUIRED = new ParseException(DynamicOTUplinkServlet.class, "Query parameter ID is required");
	public static final ParseException KEYS_REQUIRED = new ParseException(DynamicOTUplinkServlet.class, "Cookie 'Key' is required");
	public static final ParseException READ_ONLY = new ParseException(DynamicOTUplinkServlet.class,
			"Only read operations are allowed [checkout, fetch, poll]");
	private static final StructuredCodec<CommitId> COMMIT_ID_CODEC = REGISTRY.get(CommitId.class);

	private final AsyncServlet servlet;
	private final OTSystem<D> otSystem;
	private final OTDriver driver;
	private final StructuredCodec<D> diffCodec;
	private final StructuredCodec<FetchData<CommitId, D>> fetchDataCodec;
	private final String prefix;

	private DynamicOTUplinkServlet(OTDriver driver, OTSystem<D> otSystem, StructuredCodec<D> diffCodec, String prefix) {
		this.servlet = getServlet();
		this.diffCodec = diffCodec;
		this.fetchDataCodec = FetchData.codec(COMMIT_ID_CODEC, diffCodec);
		this.otSystem = otSystem;
		this.driver = driver;
		this.prefix = prefix;
	}

	public static <D> DynamicOTUplinkServlet<D> create(OTDriver driver, OTSystem<D> otSystem, StructuredCodec<D> diffCodec, String prefix) {
		return new DynamicOTUplinkServlet<>(driver, otSystem, diffCodec, prefix);
	}

	private AsyncServlet getServlet() {
		return AsyncServletDecorator.onException((request, e) -> logger.warn("Request {}", request, e))
				.serve(RoutingServlet.create()
						.map(GET, "/" + CHECKOUT, request -> {
							try {
								return getUplink(request, true).checkout()
										.map(checkoutData -> jsonResponse(fetchDataCodec, checkoutData));
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						})
						.map(GET, "/" + FETCH, request -> {
							try {
								String id = request.getQueryParameter("id");
								if (id == null) {
									throw ID_REQUIRED;
								}
								CommitId currentCommitId = fromJson(COMMIT_ID_CODEC, id);
								return getUplink(request, true).fetch(currentCommitId)
										.map(fetchData -> jsonResponse(fetchDataCodec, fetchData));
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						})
						.map(GET, "/" + POLL, request -> {
							try {
								String id = request.getQueryParameter("id");
								if (id == null) {
									throw ID_REQUIRED;
								}
								CommitId currentCommitId = fromJson(COMMIT_ID_CODEC, id);
								return getUplink(request, true).poll(currentCommitId)
										.map(fetchData -> jsonResponse(fetchDataCodec, fetchData));
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						})
						.map(POST, "/" + CREATE_COMMIT, loadBody().serve(request -> {
							try {
								FetchData<CommitId, D> fetchData = fromJson(fetchDataCodec, request.getBody().getString(UTF_8));
								return getUplink(request, false).createProtoCommit(fetchData.getCommitId(), fetchData.getDiffs(), fetchData.getLevel())
										.map(commit -> {
											assert commit.getSerializedData() != null;
											return HttpResponse.ok200()
													.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(PLAIN_TEXT)))
													.withBody(commit.getSerializedData());
										});
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						}))
						.map(POST, "/" + PUSH, loadBody().serve(request -> {
							try {
								OTUplinkImpl<CommitId, D, OTCommit<CommitId, D>> uplink = getUplink(request, false);
								OTCommit<CommitId, D> commit = ((OTRepositoryAdapter<D>) uplink.getRepository()).parseRawBytes(request.getBody().getArray());
								return uplink.push(commit)
										.map(fetchData -> jsonResponse(fetchDataCodec, fetchData));
							} catch (ParseException e) {
								return Promise.ofException(e);
							}
						})));
	}

	private static <T> HttpResponse jsonResponse(StructuredCodec<T> codec, T item) {
		return HttpResponse.ok200()
				.withHeader(CONTENT_TYPE, ofContentType(ContentType.of(JSON)))
				.withBody(toJson(codec, item).getBytes(UTF_8));
	}

	private OTUplinkImpl<CommitId, D, OTCommit<CommitId, D>> getUplink(HttpRequest request, boolean read) throws ParseException {
		String pubKeyParameter = request.getPathParameters().get("pubKey");
		String suffix = request.getPathParameters().get("suffix");
		String repositoryName = prefix + (suffix == null ? "" : ('/' + suffix));
		KeyPair keys;
		if (pubKeyParameter == null) {
			String key = request.getCookie("Key");
			if (key == null) throw KEYS_REQUIRED;
			keys = PrivKey.fromString(key).computeKeys();
		} else {
			if (!read) throw READ_ONLY;
			//noinspection ConstantConditions - will be used for read operations
			keys = new KeyPair(null, PubKey.fromString(pubKeyParameter));
		}
		RepoID repoID = RepoID.of(keys, repositoryName);
		MyRepositoryId<D> myRepositoryId = new MyRepositoryId<>(repoID, keys.getPrivKey(), diffCodec);
		OTRepositoryAdapter<D> adapter = new OTRepositoryAdapter<>(driver, myRepositoryId, emptySet());
		return OTUplinkImpl.create(adapter, otSystem);
	}

	@NotNull
	@Override
	public Async<HttpResponse> serve(@NotNull HttpRequest request) throws UncheckedException {
		return servlet.serve(request);
	}

	// for debug purposes etc.
	public DynamicOTGraphServlet<D> createGraphServlet(Function<D, String> diffToString) {
		return DynamicOTGraphServlet.create(driver, otSystem, diffCodec, prefix, diffToString);
	}
}
