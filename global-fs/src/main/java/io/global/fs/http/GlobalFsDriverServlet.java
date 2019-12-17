package io.global.fs.http;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.parse.ParseException;
import io.datakernel.http.*;
import io.datakernel.promise.Promise;
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
import static io.global.fs.util.HttpDataFormats.httpUpload;
import static io.global.fs.util.HttpDataFormats.parseRevision;
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
		String simKeyString = request.getCookie("Sim-Key");
		return simKeyString != null ? SimKey.fromString(simKeyString) : null;
	}

	public static RoutingServlet create(GlobalFsDriver driver) {
		return RoutingServlet.create()
				.map(GET, "/download/*", request -> {
					PubKey space = request.getAttachment(PubKey.class);
					return doDownload(driver, request, space);
				})
				.map(POST, "/upload", request -> {
					try {
						KeyPair keys = request.getAttachment(KeyPair.class);
						assert keys != null : "Key pair should be attached to request";
						SimKey simKey = getSimKey(request);
						return httpUpload(request, (name, offset, revision) -> driver.upload(keys, name, offset, revision, simKey));
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				})
				.map("/list", request -> {
					PubKey space = request.getAttachment(PubKey.class);
					String glob = request.getQueryParameter("glob");
					return driver.listEntities(space, glob != null ? glob : "**")
							.map(list -> HttpResponse.ok200()
									.withBody(toJson(LIST_CODEC, list).getBytes(UTF_8))
									.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(JSON))));
				})
				.map("/getMetadata/*", request -> {
					PubKey space = request.getAttachment(PubKey.class);
					return driver.getMetadata(space, request.getRelativePath())
							.map(list -> HttpResponse.ok200()
									.withBody(toJson(NULLABLE_CHECKPOINT_CODEC, list).getBytes(UTF_8))
									.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(JSON))));

				})
				.map(POST, "/delete/*", request -> {
					try {
						KeyPair keys = request.getAttachment(KeyPair.class);
						assert keys != null : "Key pair should be attached to request";
						String name = request.getRelativePath();
						return driver.delete(keys, name, parseRevision(request))
								.map($ -> HttpResponse.ok200());
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				});
	}

	@NotNull
	private static Promise<HttpResponse> doDownload(GlobalFsDriver driver, @NotNull HttpRequest request, PubKey space) {
		try {
			SimKey simKey = getSimKey(request);
			String name = UrlParser.urlDecode(request.getRelativePath());
			if (name == null) { // name is not utf so such file wont exist too, huh
				return Promise.ofException(FILE_NOT_FOUND);
			}
			return driver.getMetadata(space, name)
					.then(meta -> {
						if (meta == null) {
							return Promise.ofException(FILE_NOT_FOUND);
						}
						return HttpResponse.file(
								(offset, limit) -> driver.download(space, name, offset, limit)
										.map(supplier -> supplier
												.transformWith(CipherTransformer.create(simKey,
														CryptoUtils.nonceFromString(name), offset))),
								name,
								meta.getPosition(),
								request.getHeader(HttpHeaders.RANGE));
					});
		} catch (ParseException e) {
			return Promise.ofException(HttpException.ofCode(400, e));
		}
	}

}
