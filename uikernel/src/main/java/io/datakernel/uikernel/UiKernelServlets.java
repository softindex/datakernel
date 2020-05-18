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

package io.datakernel.uikernel;

import com.google.gson.Gson;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.common.exception.parse.ParseException;
import io.datakernel.http.*;
import io.datakernel.promise.Promise;

import java.util.List;

import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static io.datakernel.http.HttpMethod.*;
import static io.datakernel.uikernel.Utils.deserializeUpdateRequest;
import static io.datakernel.uikernel.Utils.fromJson;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Rest API for UiKernel Tables
 */
public class UiKernelServlets {
	private static final ContentType JSON_UTF8 = ContentType.of(MediaTypes.JSON, UTF_8);

	private static final String ID_PARAMETER_NAME = "id";

	public static <K, R extends AbstractRecord<K>> RoutingServlet apiServlet(GridModel<K, R> model, Gson gson) {
		return RoutingServlet.create()
				.map(POST, "/", create(model, gson))
				.map(GET, "/", read(model, gson))
				.map(PUT, "/", update(model, gson))
				.map(DELETE, "/:" + ID_PARAMETER_NAME, delete(model, gson))
				.map(GET, "/:" + ID_PARAMETER_NAME, get(model, gson));
	}

	public static <K, R extends AbstractRecord<K>> AsyncServlet read(GridModel<K, R> model, Gson gson) {
		return request -> {
			try {
				ReadSettings<K> settings = ReadSettings.from(gson, request);
				return model.read(settings).map(response ->
						createResponse(response.toJson(gson, model.getRecordType(), model.getIdType())));
			} catch (ParseException e) {
				return Promise.ofException(HttpException.ofCode(400, e));

			}
		};
	}

	public static <K, R extends AbstractRecord<K>> AsyncServlet get(GridModel<K, R> model, Gson gson) {
		return request -> {
			try {
				ReadSettings<K> settings = ReadSettings.from(gson, request);
				K id = fromJson(gson, request.getPathParameter(ID_PARAMETER_NAME), model.getIdType());
				return model.read(id, settings).map(obj ->
						createResponse(gson.toJson(obj, model.getRecordType())));
			} catch (ParseException e) {
				return Promise.ofException(HttpException.ofCode(400, e));
			}
		};
	}

	public static <K, R extends AbstractRecord<K>> AsyncServlet create(GridModel<K, R> model, Gson gson) {
		return loadBody()
				.serve(request -> {
					ByteBuf body = request.getBody();
					try {
						String json = body.asString(UTF_8);
						R obj = fromJson(gson, json, model.getRecordType());
						return model.create(obj).map(response ->
								createResponse(response.toJson(gson, model.getIdType())));
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				});
	}

	public static <K, R extends AbstractRecord<K>> AsyncServlet update(GridModel<K, R> model, Gson gson) {
		return loadBody()
				.serve(request -> {
					ByteBuf body = request.getBody();
					try {
						String json = body.asString(UTF_8);
						List<R> list = deserializeUpdateRequest(gson, json, model.getRecordType(), model.getIdType());
						return model.update(list).map(result ->
								createResponse(result.toJson(gson, model.getRecordType(), model.getIdType())));
					} catch (ParseException e) {
						return Promise.ofException(HttpException.ofCode(400, e));
					}
				});
	}

	public static <K, R extends AbstractRecord<K>> AsyncServlet delete(GridModel<K, R> model, Gson gson) {
		return request -> {
			try {
				K id = fromJson(gson, request.getPathParameter("id"), model.getIdType());
				return model.delete(id).map(response -> {
					HttpResponse res = HttpResponse.ok200();
					if (response.hasErrors()) {
						String json = gson.toJson(response.getErrors());
						res.addHeader(CONTENT_TYPE, ofContentType(JSON_UTF8));
						res.setBody(ByteBufStrings.wrapUtf8(json));
					}
					return res;
				});
			} catch (ParseException e) {
				return Promise.ofException(HttpException.ofCode(400, e));
			}
		};
	}

	private static HttpResponse createResponse(String body) {
		return HttpResponse.ok200()
				.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF8))
				.withBody(ByteBufStrings.wrapUtf8(body));
	}
}
