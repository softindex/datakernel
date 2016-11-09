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
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.cube.CubeQuery;
import io.datakernel.cube.ICube;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;

import java.lang.reflect.Type;
import java.util.Map;

import static com.google.common.collect.Maps.newLinkedHashMap;
import static io.datakernel.cube.api.CommonUtils.createGsonBuilder;
import static io.datakernel.cube.api.HttpJsonConstants.*;

public final class CubeHttpClient implements ICube {
	private final Eventloop eventloop;
	private final String url;
	private final IAsyncHttpClient httpClient;
	private final int timeout;
	private Gson gson;
	private final Map<String, Type> attributeTypes = newLinkedHashMap();
	private final Map<String, Type> measureTypes = newLinkedHashMap();

	private CubeHttpClient(Eventloop eventloop, IAsyncHttpClient httpClient,
	                       String url, int timeout) {
		this.eventloop = eventloop;
		this.url = url.replaceAll("/$", "");
		this.httpClient = httpClient;
		this.timeout = timeout;
		this.gson = createGsonBuilder(attributeTypes, measureTypes).create();
	}

	public static CubeHttpClient create(Eventloop eventloop, String domain, AsyncHttpClient httpClient, int timeout) {
		return new CubeHttpClient(eventloop, httpClient, domain, timeout);
	}

	public CubeHttpClient withDimension(String dimension, Type type) {
		attributeTypes.put(dimension, type);
		return this;
	}

	public CubeHttpClient withAttribute(String dimension, String attribute, Type type) {
		attributeTypes.put(dimension + "." + attribute, type);
		return this;
	}

	public CubeHttpClient withMeasure(String measureId, Class<?> type) {
		measureTypes.put(measureId, type);
		return this;
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
	public void query(CubeQuery query, final ResultCallback<QueryResult> callback) {
		httpClient.send(buildRequest(query), timeout, new ForwardingResultCallback<HttpResponse>(callback) {
			@Override
			public void onResult(HttpResponse httpResponse) {
				String response;
				try {
					response = ByteBufStrings.decodeUtf8(httpResponse.getBody());
				} catch (ParseException e) {
					callback.setException(new ParseException("Cube HTTP query failed. Invalid data received", e));
					return;
				}

				if (httpResponse.getCode() != 200) {
					callback.setException(new ParseException("Cube HTTP query failed. Response code: "
							+ httpResponse.getCode() + " Body: " + response));
					return;
				}

				QueryResult result = gson.fromJson(response, QueryResult.class);
				callback.setResult(result);
			}
		});
	}

	private HttpRequest buildRequest(CubeQuery query) {
		Map<String, String> urlParams = newLinkedHashMap();

		urlParams.put(ATTRIBUTES_PARAM, gson.toJson(query.getAttributes()));
		urlParams.put(MEASURES_PARAM, gson.toJson(query.getMeasures()));
		urlParams.put(FILTERS_PARAM, gson.toJson(query.getPredicate()));
		urlParams.put(SORT_PARAM, gson.toJson(query.getOrderings()));
		urlParams.put(HAVING_PARAM, gson.toJson(query.getHaving()));
		if (query.getLimit() != null)
			urlParams.put(LIMIT_PARAM, query.getLimit().toString());
		if (query.getOffset() != null)
			urlParams.put(OFFSET_PARAM, query.getOffset().toString());

		String url = this.url + "/" + "?" + HttpUtils.urlQueryString(urlParams);

		return HttpRequest.get(url);
	}
}
