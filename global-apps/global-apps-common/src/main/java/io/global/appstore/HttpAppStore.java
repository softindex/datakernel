package io.global.appstore;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.parse.ParseException;
import io.datakernel.http.*;
import io.datakernel.promise.Promise;
import io.global.appstore.pojo.AppInfo;
import io.global.appstore.pojo.HostingInfo;
import io.global.appstore.pojo.Profile;
import io.global.appstore.pojo.User;
import io.global.common.PubKey;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;

import static io.datakernel.codec.StructuredCodecs.ofSet;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.global.Utils.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HttpAppStore implements AppStore {
	private final String appStoreUrl;
	private final IAsyncHttpClient httpClient;

	private HttpAppStore(String appStoreUrl, IAsyncHttpClient httpClient) {
		this.appStoreUrl = appStoreUrl.endsWith("/") ? appStoreUrl : (appStoreUrl + "/");
		this.httpClient = httpClient;
	}

	public static HttpAppStore create(String appStoreUrl, IAsyncHttpClient httpClient) {
		return new HttpAppStore(appStoreUrl, httpClient);
	}

	@Override
	public Promise<Profile> exchangeAuthToken(String authToken) {
		return httpClient.request(HttpRequest.get(
				appStoreUrl + UrlBuilder.relative()
						.appendPathPart("api")
						.appendPathPart("auth")
						.appendPathPart("exchangeToken")
						.appendQuery("token", authToken)
						.build()))
				.then(response -> tryParseResponse(response, PROFILE_CODEC));
	}

	@Override
	public Promise<@Nullable User> findUserByPublicKey(PubKey pubKey) {
		return httpClient.request(HttpRequest.get(
				appStoreUrl + UrlBuilder.relative()
						.appendPathPart("api")
						.appendPathPart("users")
						.appendPathPart(pubKey.asString())
						.build()))
				.then(response -> tryParseResponse(response, USER_CODEC.nullable()));
	}

	@Override
	public Promise<Map<PubKey, User>> lookUp(String lookUpString, @Nullable Integer limit, @Nullable Integer offset) {
		UrlBuilder urlBuilder = UrlBuilder.relative()
				.appendPathPart("api")
				.appendPathPart("users")
				.appendPathPart("lookUp")
				.appendQuery("query", lookUpString);
		if (limit != null) {
			urlBuilder.appendQuery("limit", limit);
		}
		if (offset != null) {
			urlBuilder.appendQuery("offset", offset);
		}
		return httpClient.request(HttpRequest.get(appStoreUrl + urlBuilder.build()))
				.then(response -> tryParseResponse(response, USERS_CODEC));
	}

	@Override
	public Promise<Set<AppInfo>> listApps() {
		return httpClient.request(
				HttpRequest.get(
						appStoreUrl + UrlBuilder.relative()
								.appendPathPart("api")
								.appendPathPart("apps")
								.appendPathPart("list")
								.build()
				))
				.then(response -> tryParseResponse(response, ofSet(APP_INFO_CODEC)));
	}

	@Override
	public Promise<Set<HostingInfo>> listHostings() {
		return httpClient.request(
				HttpRequest.get(
						appStoreUrl + UrlBuilder.relative()
								.appendPathPart("api")
								.appendPathPart("hostings")
								.appendPathPart("list")
								.build()
				))
				.then(response -> tryParseResponse(response, ofSet(HOSTING_INFO_CODEC)));
	}

	private <T> Promise<T> tryParseResponse(HttpResponse response, StructuredCodec<T> codec) {
		return response.loadBody()
				.then(body -> {
					switch (response.getCode()) {
						case 200:
							try {
								return Promise.of(fromJson(codec, body.getString(UTF_8)));
							} catch (ParseException e) {
								return Promise.ofException(HttpException.ofCode(400, e));
							}
						case 404:
							return Promise.of(null);
						default:
							return Promise.ofException(HttpException.ofCode(response.getCode(), body.getString(UTF_8)));
					}
				});
	}
}
