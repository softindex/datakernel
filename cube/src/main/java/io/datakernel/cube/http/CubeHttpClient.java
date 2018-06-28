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

package io.datakernel.cube.http;

import com.google.gson.TypeAdapter;
import io.datakernel.aggregation.AggregationPredicate;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.cube.CubeQuery;
import io.datakernel.cube.ICube;
import io.datakernel.cube.QueryResult;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpUtils;
import io.datakernel.http.IAsyncHttpClient;
import io.datakernel.util.gson.GsonAdapters.TypeAdapterMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.datakernel.cube.http.Utils.*;
import static io.datakernel.util.LogUtils.toLogger;

public final class CubeHttpClient implements ICube {
	protected final Logger logger = LoggerFactory.getLogger(CubeHttpClient.class);

	private final Eventloop eventloop;
	private final String url;
	private final IAsyncHttpClient httpClient;
	private final TypeAdapterMapping mapping;
	private TypeAdapter<QueryResult> queryResultJson;
	private TypeAdapter<AggregationPredicate> aggregationPredicateJson;
	private final Map<String, Type> attributeTypes = new LinkedHashMap<>();
	private final Map<String, Type> measureTypes = new LinkedHashMap<>();

	private CubeHttpClient(Eventloop eventloop, IAsyncHttpClient httpClient, String url, TypeAdapterMapping mapping) {
		this.eventloop = eventloop;
		this.url = url.replaceAll("/$", "");
		this.httpClient = httpClient;
		this.mapping = mapping;
	}

	public static CubeHttpClient create(Eventloop eventloop, AsyncHttpClient httpClient, String cubeServletUrl) {
		return new CubeHttpClient(eventloop, httpClient, cubeServletUrl, CUBE_TYPES);
	}

	public static CubeHttpClient create(Eventloop eventloop, AsyncHttpClient httpClient, URI cubeServletUrl) {
		return create(eventloop, httpClient, cubeServletUrl.toString());
	}

	public CubeHttpClient withAttribute(String attribute, Type type) {
		attributeTypes.put(attribute, type);
		return this;
	}

	public CubeHttpClient withMeasure(String measureId, Class<?> type) {
		measureTypes.put(measureId, type);
		return this;
	}

	private TypeAdapter<AggregationPredicate> getAggregationPredicateJson() {
		if (aggregationPredicateJson == null) {
			aggregationPredicateJson = AggregationPredicateGsonAdapter.create(mapping, attributeTypes, measureTypes);
		}
		return aggregationPredicateJson;
	}

	private TypeAdapter<QueryResult> getQueryResultJson() {
		if (queryResultJson == null) {
			queryResultJson = QueryResultGsonAdapter.create(mapping, attributeTypes, measureTypes);
		}
		return queryResultJson;
	}

	@Override
	public Map<String, Type> getAttributeTypes() {
		return attributeTypes;
	}

	@Override
	public Map<String, Type> getMeasureTypes() {
		return measureTypes;
	}

	@Override
	public Stage<QueryResult> query(CubeQuery query) {
		return httpClient.send(buildRequest(query))
				.thenCompose(httpResponse -> {
					String response;
					try {
						response = ByteBufStrings.decodeUtf8(httpResponse.getBody()); // TODO getBodyAsString
					} catch (ParseException e) {
						return Stage.ofException(new ParseException("Cube HTTP query failed. Invalid data received", e));
					}

					if (httpResponse.getCode() != 200) {
						return Stage.ofException(new ParseException("Cube HTTP query failed. Response code: " + httpResponse.getCode() + " Body: " + response));
					}

					QueryResult result;
					try {
						result = getQueryResultJson().fromJson(response);
					} catch (IOException e) {
						return Stage.ofException(new ParseException("Cube HTTP query failed. Invalid data received", e));
					}
					return Stage.of(result);
				})
				.whenComplete(toLogger(logger, "query", query));
	}

	private HttpRequest buildRequest(CubeQuery query) {
		Map<String, String> urlParams = new LinkedHashMap<>();

		urlParams.put(ATTRIBUTES_PARAM, query.getAttributes().stream().collect(Collectors.joining(",")));
		urlParams.put(MEASURES_PARAM, query.getMeasures().stream().collect(Collectors.joining(",")));
		urlParams.put(WHERE_PARAM, getAggregationPredicateJson().toJson(query.getWhere()));
		urlParams.put(SORT_PARAM, formatOrderings(query.getOrderings()));
		urlParams.put(HAVING_PARAM, getAggregationPredicateJson().toJson(query.getHaving()));
		if (query.getLimit() != null)
			urlParams.put(LIMIT_PARAM, query.getLimit().toString());
		if (query.getOffset() != null)
			urlParams.put(OFFSET_PARAM, query.getOffset().toString());
		urlParams.put(REPORT_TYPE_PARAM, query.getReportType().toString().toLowerCase());
		String url = this.url + "/" + "?" + HttpUtils.renderQueryString(urlParams);

		return HttpRequest.get(url);
	}
}
