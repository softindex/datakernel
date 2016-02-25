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
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.gson.QueryPredicatesGsonSerializer;
import io.datakernel.async.ResultCallback;
import io.datakernel.cube.CubeQuery;
import io.datakernel.http.*;

import java.util.HashMap;
import java.util.Map;

import static io.datakernel.cube.api.CubeHttpServer.QUERY_REQUEST_PATH;
import static io.datakernel.cube.api.HttpJsonConstants.*;
import static io.datakernel.util.ByteBufStrings.decodeUTF8;

public final class CubeHttpClient {
	private final String domain;
	private final AsyncHttpClient httpClient;
	private final int timeout;
	private final Gson gson;

	public CubeHttpClient(String domain, AsyncHttpClient httpClient, int timeout, AggregationStructure structure,
	                      ReportingConfiguration reportingConfiguration) {
		this.domain = domain.replaceAll("/$", "");
		this.httpClient = httpClient;
		this.timeout = timeout;
		this.gson = new GsonBuilder()
				.registerTypeAdapter(AggregationQuery.Predicates.class, new QueryPredicatesGsonSerializer(structure))
				.registerTypeAdapter(ReportingQueryResult.class, new ReportingQueryResponseDeserializer(structure, reportingConfiguration))
				.registerTypeAdapter(CubeQuery.Ordering.class, new QueryOrderingGsonSerializer())
				.create();
	}

	public void query(ReportingQuery query, final ResultCallback<ReportingQueryResult> callback) {
		httpClient.execute(buildRequest(query), timeout, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse httpResponse) {
				String response = decodeUTF8(httpResponse.getBody());

				if (httpResponse.getCode() != 200) {
					callback.onException(new Exception("Cube HTTP query failed. Response code: "
							+ httpResponse.getCode() + " Body: " + response));
					return;
				}

				try {
					ReportingQueryResult result = gson.fromJson(response, ReportingQueryResult.class);
					callback.onResult(result);
				} catch (Exception e) {
					callback.onException(new Exception("Could not parse cube HTTP query response", e));
				}
			}

			@Override
			public void onException(Exception e) {
				callback.onException(new Exception("Cube HTTP request failed", e));
			}
		});
	}

	private HttpRequest buildRequest(ReportingQuery query) {
		Map<String, String> urlParams = new HashMap<>();

		if (query.getDimensions() != null)
			urlParams.put(DIMENSIONS_PARAM, gson.toJson(query.getDimensions()));

		if (query.getMeasures() != null)
			urlParams.put(MEASURES_PARAM, gson.toJson(query.getMeasures()));

		if (query.getAttributes() != null)
			urlParams.put(ATTRIBUTES_PARAM, gson.toJson(query.getAttributes()));

		if (query.getFilters() != null)
			urlParams.put(FILTERS_PARAM, gson.toJson(query.getFilters()));

		if (query.getSort() != null)
			urlParams.put(SORT_PARAM, gson.toJson(query.getSort()));

		if (query.getLimit() != null)
			urlParams.put(LIMIT_PARAM, query.getLimit().toString());

		if (query.getOffset() != null)
			urlParams.put(OFFSET_PARAM, query.getOffset().toString());

		if (query.getSearchString() != null)
			urlParams.put(SEARCH_PARAM, query.getSearchString());

		if (query.getFields() != null)
			urlParams.put(FIELDS_PARAM, gson.toJson(query.getFields()));

		if (query.getMetadataFields() != null)
			urlParams.put(METADATA_FIELDS_PARAM, gson.toJson(query.getMetadataFields()));

		String url = domain + QUERY_REQUEST_PATH + "?" + HttpUtils.urlQueryString(urlParams);

		return HttpRequest.get(url);
	}
}
