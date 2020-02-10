package io.global;

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.common.parse.ParseException;
import io.datakernel.common.tuple.Tuple2;
import io.datakernel.config.Config;
import io.datakernel.http.AsyncServletDecorator;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.ot.TransformResult;
import io.datakernel.promise.Promise;
import io.global.appstore.pojo.AppInfo;
import io.global.appstore.pojo.HostingInfo;
import io.global.appstore.pojo.Profile;
import io.global.appstore.pojo.User;
import io.global.common.CryptoUtils;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.api.AnnounceData;
import org.spongycastle.math.ec.ECPoint;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.config.ConfigConverters.ofDuration;
import static io.datakernel.http.AsyncServletDecorator.mapResponse;
import static io.datakernel.http.HttpHeaders.*;
import static io.global.common.CryptoUtils.randomBytes;
import static io.global.common.CryptoUtils.toHexString;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final Config DEFAULT_SYNC_SCHEDULE_CONFIG = Config.create()
			.with("type", "interval")
			.with("value", Config.ofValue(ofDuration(), Duration.ofSeconds(30)));
	public static final StructuredCodec<PubKey> PUB_KEY_HEX_CODEC = STRING_CODEC.transform(PubKey::fromString, PubKey::asString);
	public static final StructuredCodec<PrivKey> PRIV_KEY_HEX_CODEC = STRING_CODEC.transform(PrivKey::fromString, PrivKey::asString);
	public static final StructuredCodec<AnnounceData> ANNOUNCE_DATA_CODEC = REGISTRY.get(AnnounceData.class);
	public static final StructuredCodec<AppInfo> APP_INFO_CODEC = object(AppInfo::new,
			"id", AppInfo::getId, INT_CODEC,
			"name", AppInfo::getName, STRING_CODEC,
			"description", AppInfo::getDescription, STRING_CODEC,
			"logoUrl", AppInfo::getLogoUrl, STRING_CODEC);
	public static final StructuredCodec<HostingInfo> HOSTING_INFO_CODEC = object(HostingInfo::new,
			"id", HostingInfo::getId, INT_CODEC,
			"name", HostingInfo::getName, STRING_CODEC,
			"description", HostingInfo::getDescription, STRING_CODEC.nullable(),
			"logoUrl", HostingInfo::getLogoUrl, STRING_CODEC.nullable(),
			"terms", HostingInfo::getTerms, STRING_CODEC.nullable());
	public static final StructuredCodec<User> USER_CODEC = object(User::new,
			"username", User::getUsername, STRING_CODEC,
			"firstName", User::getFirstName, STRING_CODEC.nullable(),
			"lastName", User::getLastName, STRING_CODEC.nullable());
	public static final StructuredCodec<Profile> PROFILE_CODEC = object(Profile::new,
			"pubKey", Profile::getPubKey, PUB_KEY_HEX_CODEC,
			"user", Profile::getUser, USER_CODEC,
			"email", Profile::getEmail, STRING_CODEC);
	public static final StructuredCodec<Map<PubKey, User>> USERS_CODEC = ofMapAsObjectList(
			"pubKey", PUB_KEY_HEX_CODEC,
			"profile", USER_CODEC);

	public static String generateString(int size) {
		return toHexString(randomBytes(size));
	}

	public static <K, V> StructuredCodec<Map<K, V>> ofMapAsObjectList(
			String keyFieldName, StructuredCodec<K> keyCodec,
			String valueFieldName, StructuredCodec<V> valueCodec) {
		//noinspection RedundantTypeArguments - Cannot infer
		return StructuredCodecs.<Tuple2<K, V>>ofList(
				object(Tuple2::new,
						keyFieldName, Tuple2::getValue1, keyCodec,
						valueFieldName, Tuple2::getValue2, valueCodec))
				.transform(tuples -> tuples
								.stream()
								.collect(toMap(Tuple2::getValue1, Tuple2::getValue2)),
						map -> map.entrySet()
								.stream()
								.map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
								.collect(toList()));
	}

	public static HttpResponse redirectToReferer(HttpRequest request, String defaultPath) {
		String referer = request.getHeader(REFERER);
		return HttpResponse.redirect302(referer != null ? referer : defaultPath);
	}

	public static <T, E> BiFunction<T, Throwable, Promise<T>> revertIfException(AsyncSupplier<E> undo) {
		return (result, e) -> {
			if (e == null) {
				return Promise.of(result);
			}
			return undo.get()
					.thenEx(($2, e2) -> {
						if (e2 != null) {
							e.addSuppressed(e2);
						}
						return Promise.ofException(e);
					});
		};
	}

	private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();
	private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();

	public static String pubKeyToBase64(PubKey pubKey) {
		ECPoint point = pubKey.getEcPublicKey().getQ();
		byte[] first = point.getXCoord().toBigInteger().toByteArray();
		byte[] second = point.getYCoord().toBigInteger().toByteArray();
		return BASE64_ENCODER.encodeToString(first) + ':' + BASE64_ENCODER.encodeToString(second);
	}

	public static PubKey base64ToPubKey(String string) throws ParseException {
		String[] parts = string.split(":");
		if (parts.length != 2) {
			throw new ParseException(PubKey.class, "No or more than one ':' delimiters in public key string");
		}
		try {
			BigInteger x = new BigInteger(BASE64_DECODER.decode(parts[0]));
			BigInteger y = new BigInteger(BASE64_DECODER.decode(parts[1]));
			try {
				return PubKey.of(CryptoUtils.CURVE.getCurve().validatePoint(x, y));
			} catch (IllegalArgumentException | ArithmeticException e) {
				throw new ParseException(PubKey.class, "Failed to read a point on elliptic curve", e);
			}
		} catch (NumberFormatException e) {
			throw new ParseException(PubKey.class, "Failed to parse big integer", e);
		}
	}

	public static <O, S> TransformResult<O> collect(TransformResult<S> subResult, Function<S, O> constructor) {
		return TransformResult.of(doCollect(subResult.left, constructor), doCollect(subResult.right, constructor));
	}

	public static <O, S> List<O> doCollect(List<S> ops, Function<S, O> constructor) {
		return ops.stream()
				.map(constructor)
				.collect(toList());
	}

	public static AsyncServletDecorator cachedContent(int maxAgeSeconds) {
		return mapResponse((r, response) -> response.getCode() == 200 ?
				response.withHeader(CACHE_CONTROL, "public, immutable, max-age=" + maxAgeSeconds) :
				response);
	}

	// 1 year
	public static AsyncServletDecorator cachedContent() {
		return cachedContent(31536000);
	}

	public static boolean isGzipAccepted(HttpRequest request) {
		String header = request.getHeader(ACCEPT_ENCODING);
		if (header == null) {
			return false;
		}
		for (String part : header.split(",")) {
			String encoding = part.split(";")[0].trim();
			if (encoding.equals("gzip") || encoding.equals("*")) {
				return true;
			}
		}
		return false;
	}
}
