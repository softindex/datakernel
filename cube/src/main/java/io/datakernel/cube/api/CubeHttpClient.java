/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.cube.api;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.gson.QueryPredicatesGsonSerializer;
import io.datakernel.async.ResultCallback;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.HttpUtils;
import io.datakernel.util.ByteBufStrings;

import java.util.HashMap;
import java.util.Map;

import static io.datakernel.cube.api.CubeHttpServer.REPORTING_QUERY_REQUEST_PATH;

public final class CubeHttpClient {
	private final String domain;
	private final AsyncHttpClient httpClient;
	private final int timeout;
	private final Gson gson;

	public CubeHttpClient(String domain, AsyncHttpClient httpClient, int timeout, AggregationStructure structure) {
		this.domain = domain.replaceAll("/$", "");
		this.httpClient = httpClient;
		this.timeout = timeout;
		this.gson = new GsonBuilder()
				.registerTypeAdapter(AggregationQuery.QueryPredicates.class, new QueryPredicatesGsonSerializer(structure))
				.registerTypeAdapter(ReportingQueryResult.class, new ReportingQueryResponseDeserializer(structure))
				.create();
	}

	public void query(CubeHttpQuery query, final ResultCallback<ReportingQueryResult> callback) {
		httpClient.execute(buildRequest(query), timeout, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse httpResponse) {
				if (httpResponse.getCode() != 200) {
					callback.onException(new Exception("Cube HTTP query failed. Response code: " + httpResponse.getCode()));
					return;
				}

				try {
					String response = ByteBufStrings.decodeUTF8(httpResponse.getBody());
					ReportingQueryResult result = gson.fromJson(response, ReportingQueryResult.class);
					callback.onResult(result);
				} catch (Exception e) {
					callback.onException(new Exception("Could not parse cube HTTP query response"));
				}
			}

			@Override
			public void onException(Exception exception) {
				callback.onException(new Exception("Cube HTTP request failed"));
			}
		});
	}

	private HttpRequest buildRequest(CubeHttpQuery query) {
		Map<String, String> urlParams = new HashMap<>();

		if (query.getDimensions() != null)
			urlParams.put("dimensions", gson.toJson(query.getDimensions()));

		if (query.getMeasures() != null)
			urlParams.put("measures", gson.toJson(query.getMeasures()));

		if (query.getAttributes() != null)
			urlParams.put("attributes", gson.toJson(query.getAttributes()));

		if (query.getFilters() != null)
			urlParams.put("filters", gson.toJson(query.getFilters()));

		if (query.getSort() != null)
			urlParams.put("sort", toJson(query.getSort()));

		if (query.getLimit() != null)
			urlParams.put("limit", query.getLimit().toString());

		if (query.getOffset() != null)
			urlParams.put("offset", query.getOffset().toString());

		String url = domain + REPORTING_QUERY_REQUEST_PATH + "?" + HttpUtils.urlQueryString(urlParams);

		return HttpRequest.get(url);
	}

	private String toJson(AggregationQuery.QueryOrdering ordering) {
		JsonArray jsonArray = new JsonArray();
		jsonArray.add(new JsonPrimitive(ordering.getPropertyName()));
		jsonArray.add(new JsonPrimitive(ordering.isAsc() ? "asc" : "desc"));
		return jsonArray.toString();
	}
}
