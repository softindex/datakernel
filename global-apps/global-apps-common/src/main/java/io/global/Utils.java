package io.global;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.util.Tuple2;
import io.global.appstore.pojo.AppInfo;
import io.global.appstore.pojo.HostingInfo;
import io.global.appstore.pojo.Profile;
import io.global.appstore.pojo.User;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.api.AnnounceData;

import java.util.Map;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.global.common.CryptoUtils.randomBytes;
import static io.global.common.CryptoUtils.toHexString;
import static io.global.ot.util.BinaryDataFormats.REGISTRY;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public final class Utils {
	private Utils() {
		throw new AssertionError();
	}

	public static final StructuredCodec<PubKey> PUB_KEY_HEX_CODEC = STRING_CODEC.transform(PubKey::fromString, PubKey::asString);
	public static final StructuredCodec<PrivKey> PRIV_KEY_HEX_CODEC = STRING_CODEC.transform(PrivKey::fromString, PrivKey::asString);
	public static final StructuredCodec<AnnounceData> ANNOUNCE_DATA_CODEC = REGISTRY.get(AnnounceData.class);
	public static final StructuredCodec<AppInfo> APP_INFO_CODEC = object(AppInfo::new,
			"id", AppInfo::getId, INT_CODEC,
			"name", AppInfo::getName, STRING_CODEC,
			"description", AppInfo::getDescription, STRING_CODEC);
	public static final StructuredCodec<HostingInfo> HOSTING_INFO_CODEC = object(HostingInfo::new,
			"id", HostingInfo::getId, INT_CODEC,
			"name", HostingInfo::getName, STRING_CODEC,
			"description", HostingInfo::getDescription, STRING_CODEC.nullable(),
			"logoUrl", HostingInfo::getLogoUrl, STRING_CODEC.nullable(),
			"terms", HostingInfo::getTerms, STRING_CODEC.nullable());
	public static final StructuredCodec<User> USER_CODEC = object(User::new,
			"username", User::getUsername, STRING_CODEC,
			"firstName", User::getFirstName, STRING_CODEC,
			"lastName", User::getLastName, STRING_CODEC);
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

}
