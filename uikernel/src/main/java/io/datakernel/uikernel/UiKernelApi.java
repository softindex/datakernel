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
import io.datakernel.async.ResultCallback;
import io.datakernel.http.ContentType;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.MiddlewareServlet;
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
public class UiKernelApi {
	private final ContentType JSON_UTF8 = ContentType.JSON.setCharsetEncoding(Charset.forName("UTF-8"));

	public <E extends AbstractRecord<T>, T> AsyncHttpServlet createApi(GridModelManager controller, Gson gson) {
		MiddlewareServlet main = new MiddlewareServlet();
		main.get("/", getAll(controller, gson));
		main.get("/:id", get(controller, gson));
		main.post("/", create(controller, gson));
		main.use("/", PUT, update(controller, gson));
		main.use("/:id", DELETE, delete(controller, gson));
		return main;
	}

	private <E extends AbstractRecord<T>, T> AsyncHttpServlet getAll(final GridModelManager<E, T> controller, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					String query = decodeUtf8Query(req.getUrl().getQuery());
					ReadSettings settings = ReadSettings.parse(gson, query);
					controller.read(settings, new ResultCallback<ReadResponse<E, T>>() {
						@Override
						public void onResult(ReadResponse<E, T> response) {
							String json = response.toJson(gson, controller.getType(), controller.getIdType());
							callback.onResult(HttpResponse.create()
									.setContentType(JSON_UTF8)
									.body(ByteBufStrings.wrapUTF8(json)));
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

	private <E extends AbstractRecord<T>, T> AsyncHttpServlet get(final GridModelManager<E, T> controller, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					String query = decodeUtf8Query(req.getUrl().getQuery());
					ReadSettings settings = ReadSettings.parse(gson, query);
					T id = gson.fromJson(req.getUrlParameter("id"), controller.getIdType());
					controller.read(id, settings, new ResultCallback<E>() {
						@Override
						public void onResult(E obj) {
							String json = gson.toJson(obj, controller.getType());
							callback.onResult(HttpResponse.create()
									.setContentType(JSON_UTF8)
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

	private <E extends AbstractRecord<T>, T> AsyncHttpServlet create(final GridModelManager<E, T> controller, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					String json = ByteBufStrings.decodeUTF8(req.getBody());
					E obj = gson.fromJson(json, controller.getType());
					controller.create(obj, new ResultCallback<CreateResponse<T>>() {
						@Override
						public void onResult(CreateResponse<T> response) {
							String json = response.toJson(gson, controller.getIdType());
							HttpResponse res = HttpResponse.create()
									.setContentType(JSON_UTF8)
									.body(ByteBufStrings.wrapUTF8(json));
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

	private <E extends AbstractRecord<T>, T> AsyncHttpServlet update(final GridModelManager<E, T> controller, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					String json = ByteBufStrings.decodeUTF8(req.getBody());
					List<E> list = deserializeUpdateRequest(gson, json, controller.getType(), controller.getIdType());
					controller.update(list, new ResultCallback<UpdateResponse<E, T>>() {
						@Override
						public void onResult(UpdateResponse<E, T> result) {
							String json = result.toJson(gson, controller.getType(), controller.getIdType());
							callback.onResult(HttpResponse.create()
									.setContentType(JSON_UTF8)
									.body(ByteBufStrings.wrapUTF8(json)));
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

	private <E extends AbstractRecord<T>, T> AsyncHttpServlet delete(final GridModelManager<E, T> controller, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					T id = gson.fromJson(req.getUrlParameter("id"), controller.getIdType());
					controller.delete(id, new ResultCallback<DeleteResponse>() {
						@Override
						public void onResult(DeleteResponse response) {
							HttpResponse res = HttpResponse.create();
							String json = response.toJson(gson);
							if (json != null) {
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
