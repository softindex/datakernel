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

package io.datakernel.uikernel;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.datakernel.async.ResultCallback;
import io.datakernel.http.*;
import io.datakernel.http.server.AsyncHttpServlet;
import io.datakernel.util.ByteBufStrings;

import java.nio.charset.Charset;
import java.util.List;

import static io.datakernel.http.HttpMethod.DELETE;
import static io.datakernel.http.HttpMethod.PUT;
import static io.datakernel.uikernel.Utils.decodeUtf8Query;
import static io.datakernel.uikernel.Utils.deserializeUpdateRequest;

/**
 * Rest API for UiKernel Module
 */
@SuppressWarnings("unused")
public class UiKernelServlets {
	public static final ContentType JSON_UTF8 = ContentType.of(MediaType.JSON, Charset.forName("UTF-8"));

	public static <K, R extends AbstractRecord<K>> MiddlewareServlet apiServlet(GridModel model, Gson gson) {
		MiddlewareServlet main = new MiddlewareServlet();
		main.get("/", read(model, gson));
		main.get("/:id", get(model, gson));
		main.post("/", create(model, gson));
		main.use("/", PUT, update(model, gson));
		main.use("/:id", DELETE, delete(model, gson));
		return main;
	}

	public static <K, R extends AbstractRecord<K>> AsyncHttpServlet read(final GridModel<K, R> model, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					String query = decodeUtf8Query(req.getUrl().getQuery());
					ReadSettings settings = ReadSettings.parse(gson, query);
					model.read(settings, new ResultCallback<ReadResponse<K, R>>() {
						@Override
						public void onResult(ReadResponse<K, R> response) {
							JsonObject json = response.toJson(gson, model.getRecordType(), model.getIdType());
							callback.onResult(HttpResponse.create()
									.setContentType(JSON_UTF8)
									.body(ByteBufStrings.wrapUTF8(gson.toJson(json))));
						}

						@Override
						public void onException(Exception e) {
							callback.onResult(HttpResponse.create(404));
						}
					});
				} catch (Exception e) {
					callback.onResult(HttpResponse.create(400));
				}
			}
		};
	}

	public static <K, R extends AbstractRecord<K>> AsyncHttpServlet get(final GridModel<K, R> model, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					String query = decodeUtf8Query(req.getUrl().getQuery());
					ReadSettings settings = ReadSettings.parse(gson, query);
					K id = gson.fromJson(req.getUrlParameter("id"), model.getIdType());
					model.read(id, settings, new ResultCallback<R>() {
						@Override
						public void onResult(R obj) {
							String json = gson.toJson(obj, model.getRecordType());
							callback.onResult(HttpResponse.create()
//									.setContentType(JSON_UTF8)
									.body(ByteBufStrings.wrapUTF8(json)));
						}

						@Override
						public void onException(Exception e) {
							callback.onResult(HttpResponse.create(404));
						}
					});
				} catch (NumberFormatException e) {
					callback.onResult(HttpResponse.create(400));
				}
			}
		};
	}

	public static <K, R extends AbstractRecord<K>> AsyncHttpServlet create(final GridModel<K, R> model, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					String json = ByteBufStrings.decodeUTF8(req.getBody());
					R obj = gson.fromJson(json, model.getRecordType());
					model.create(obj, new ResultCallback<CreateResponse<K>>() {
						@Override
						public void onResult(CreateResponse<K> response) {
							JsonObject json = response.toJson(gson, model.getIdType());
							HttpResponse res = HttpResponse.create()
									.setContentType(JSON_UTF8)
									.body(ByteBufStrings.wrapUTF8(gson.toJson(json)));
							callback.onResult(res);
						}

						@Override
						public void onException(Exception e) {
							callback.onResult(HttpResponse.create(404));
						}
					});
				} catch (Exception e) {
					callback.onResult(HttpResponse.create(400));
				}
			}
		};
	}

	public static <K, R extends AbstractRecord<K>> AsyncHttpServlet update(final GridModel<K, R> model, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					String json = ByteBufStrings.decodeUTF8(req.getBody());
					List<R> list = deserializeUpdateRequest(gson, json, model.getRecordType(), model.getIdType());
					model.update(list, new ResultCallback<UpdateResponse<K, R>>() {
						@Override
						public void onResult(UpdateResponse<K, R> result) {
							JsonObject json = result.toJson(gson, model.getRecordType(), model.getIdType());
							callback.onResult(HttpResponse.create()
									.setContentType(JSON_UTF8)
									.body(ByteBufStrings.wrapUTF8(gson.toJson(json))));
						}

						@Override
						public void onException(Exception e) {
							callback.onResult(HttpResponse.create(404));
						}
					});
				} catch (Exception e) {
					callback.onResult(HttpResponse.create(400));
				}
			}
		};
	}

	public static <K, R extends AbstractRecord<K>> AsyncHttpServlet delete(final GridModel<K, R> model, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					K id = gson.fromJson(req.getUrlParameter("id"), model.getIdType());
					model.delete(id, new ResultCallback<DeleteResponse>() {
						@Override
						public void onResult(DeleteResponse response) {
							HttpResponse res = HttpResponse.create();
							if (response.hasErrors()) {
								String json = gson.toJson(response.getErrors());
								res.setContentType(JSON_UTF8)
										.body(ByteBufStrings.wrapUTF8(json));
							}
							callback.onResult(res);
						}

						@Override
						public void onException(Exception e) {
							callback.onResult(HttpResponse.create(404));
						}
					});
				} catch (Exception e) {
					callback.onResult(HttpResponse.create(400));
				}
			}
		};
	}
}
