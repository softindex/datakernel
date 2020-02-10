/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.aggregation.AggregationPredicate;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.common.parse.ParseException;
import io.datakernel.cube.CubeQuery;
import io.datakernel.cube.ICube;
import io.datakernel.cube.QueryResult;
import io.datakernel.http.*;
import io.datakernel.promise.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.cube.http.Utils.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class CubeHttpClient implements ICube {
	private static final Logger logger = LoggerFactory.getLogger(CubeHttpClient.class);

	private final String url;
	private final IAsyncHttpClient httpClient;
	private final CodecFactory mapping;
	private StructuredCodec<QueryResult> queryResultCodec;
	private StructuredCodec<AggregationPredicate> aggregationPredicateCodec;
	private final Map<String, Type> attributeTypes = new LinkedHashMap<>();
	private final Map<String, Type> measureTypes = new LinkedHashMap<>();

	private CubeHttpClient(IAsyncHttpClient httpClient, String url, CodecFactory mapping) {
		this.url = url.replaceAll("/$", "");
		this.httpClient = httpClient;
		this.mapping = mapping;
	}

	public static CubeHttpClient create(AsyncHttpClient httpClient, String cubeServletUrl) {
		return new CubeHttpClient(httpClient, cubeServletUrl, CUBE_TYPES);
	}

	public static CubeHttpClient create(AsyncHttpClient httpClient, URI cubeServletUrl) {
		return create(httpClient, cubeServletUrl.toString());
	}

	public CubeHttpClient withAttribute(String attribute, Type type) {
		attributeTypes.put(attribute, type);
		return this;
	}

	public CubeHttpClient withMeasure(String measureId, Class<?> type) {
		measureTypes.put(measureId, type);
		return this;
	}

	private StructuredCodec<AggregationPredicate> getAggregationPredicateCodec() {
		if (aggregationPredicateCodec == null) {
			aggregationPredicateCodec = AggregationPredicateCodec.create(mapping, attributeTypes, measureTypes);
		}
		return aggregationPredicateCodec;
	}

	private StructuredCodec<QueryResult> getQueryResultCodec() {
		if (queryResultCodec == null) {
			queryResultCodec = QueryResultCodec.create(mapping, attributeTypes, measureTypes);
		}
		return queryResultCodec;
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
	public Promise<QueryResult> query(CubeQuery query) {
		return httpClient.request(buildRequest(query))
				.then(response -> response.loadBody()
						.then(body -> {
							try {
								String httpResponse = body.getString(UTF_8);
								if (response.getCode() != 200) {
									return Promise.ofException(HttpException.ofCode(400, "Cube HTTP query failed. Response code: " + response.getCode() + " Body: " + httpResponse));
								}
								QueryResult result = fromJson(getQueryResultCodec(), httpResponse);
								return Promise.of(result);
							} catch (ParseException e) {
								return Promise.ofException(HttpException.ofCode(400, "Cube HTTP query failed. Invalid data received", e));
							}
						})
						.whenComplete(toLogger(logger, "query", query)));
	}

	private HttpRequest buildRequest(CubeQuery query) {
		Map<String, String> urlParams = new LinkedHashMap<>();

		urlParams.put(ATTRIBUTES_PARAM, String.join(",", query.getAttributes()));
		urlParams.put(MEASURES_PARAM, String.join(",", query.getMeasures()));
		urlParams.put(WHERE_PARAM, toJson(getAggregationPredicateCodec(), query.getWhere()));
		urlParams.put(SORT_PARAM, formatOrderings(query.getOrderings()));
		urlParams.put(HAVING_PARAM, toJson(getAggregationPredicateCodec(), query.getHaving()));
		if (query.getLimit() != null)
			urlParams.put(LIMIT_PARAM, query.getLimit().toString());
		if (query.getOffset() != null)
			urlParams.put(OFFSET_PARAM, query.getOffset().toString());
		urlParams.put(REPORT_TYPE_PARAM, query.getReportType().toString().toLowerCase());
		String url = this.url + "/" + "?" + HttpUtils.renderQueryString(urlParams);

		return HttpRequest.get(url);
	}
}
