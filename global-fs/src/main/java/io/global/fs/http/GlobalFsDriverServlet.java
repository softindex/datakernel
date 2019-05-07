package io.global.fs.http;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.global.common.*;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.local.GlobalFsDriver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.util.List;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.MediaTypes.JSON;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.global.fs.util.HttpDataFormats.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class GlobalFsDriverServlet {
	private static final StructuredCodec<GlobalFsCheckpoint> CHECKPOINT_CODEC = tuple(GlobalFsCheckpoint::of,
			GlobalFsCheckpoint::getFilename, STRING_CODEC,
			GlobalFsCheckpoint::getPosition, LONG_CODEC,
			GlobalFsCheckpoint::getRevision, LONG_CODEC,
			GlobalFsCheckpoint::getDigestOrNull, (STRING_CODEC.transform($ -> {
				throw new UnsupportedOperationException();
			}, digest -> {
				if (digest == null) {
					return null;
				}
				byte[] hash = new byte[digest.getDigestSize()];
				new SHA256Digest(digest).doFinal(hash, 0);
				return CryptoUtils.toHexString(hash);
			})),
			GlobalFsCheckpoint::getSimKeyHash, STRING_CODEC.transform(Hash::fromString, Hash::asString).nullable());

	private static final StructuredCodec<@Nullable GlobalFsCheckpoint> NULLABLE_CHECKPOINT_CODEC = CHECKPOINT_CODEC.nullable();
	private static final StructuredCodec<List<GlobalFsCheckpoint>> LIST_CODEC = ofList(CHECKPOINT_CODEC);

	private GlobalFsDriverServlet() {
	}

	@Nullable
	private static SimKey getSimKey(HttpRequest request) throws ParseException {
		String simKeyString = request.getCookieOrNull("Sim-Key");
		return simKeyString != null ? SimKey.fromString(simKeyString) : null;
	}

	public static RoutingServlet create(GlobalFsDriver driver) {
		return RoutingServlet.create()
				.with(GET, "/download/:space/*", request -> {
					try {
						PubKey space = PubKey.fromString(request.getPathParameter("space"));
						SimKey simKey = getSimKey(request);
						String name = request.getRelativePath();
						return driver.getMetadata(space, name)
								.then(meta -> {
									if (meta != null) {
										return httpDownload(request,
												(offset, limit) ->
														driver.download(space, name, offset, limit)
																.map(supplier -> supplier
																		.transformWith(CipherTransformer.create(simKey, CryptoUtils.nonceFromString(name), offset))),
												name, meta.getPosition());
									}
									return Promise.ofException(FILE_NOT_FOUND);
								});
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/upload", request -> {
					try {
						KeyPair keys = PrivKey.fromString(request.getCookie("Key")).computeKeys();
						SimKey simKey = getSimKey(request);
						return httpUpload(request, (name, offset, revision) -> driver.upload(keys, name, offset, revision, simKey));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with("/list/:space", request -> {
					try {
						return driver.listEntities(PubKey.fromString(request.getPathParameter("space")), request.getQueryParameter("glob", "**"))
								.map(list -> HttpResponse.ok200()
										.withBody(toJson(LIST_CODEC, list).getBytes(UTF_8))
										.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(JSON))));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with("/getMetadata/:space/*", request -> {
					try {
						return driver.getMetadata(PubKey.fromString(request.getPathParameter("space")), request.getRelativePath())
								.map(list -> HttpResponse.ok200()
										.withBody(toJson(NULLABLE_CHECKPOINT_CODEC, list).getBytes(UTF_8))
										.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(JSON))));
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				})
				.with(POST, "/delete/*", request -> {
					try {
						KeyPair keys = PrivKey.fromString(request.getCookie("Key")).computeKeys();
						String name = request.getRelativePath();
						return driver.delete(keys, name, parseRevision(request))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(e);
					}
				});
	}
}
