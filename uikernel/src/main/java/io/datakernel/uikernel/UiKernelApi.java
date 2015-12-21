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
import java.util.Map;

import static io.datakernel.http.HttpMethod.DELETE;
import static io.datakernel.http.HttpMethod.PUT;

@SuppressWarnings("unused")
public class UiKernelApi {
	private static final ContentType JSON_UTF8 = ContentType.JSON.setCharsetEncoding(Charset.forName("UTF-8"));

	public static AsyncHttpServlet getApiServlet(Controller controller, Gson gson) {
		MiddlewareServlet main = new MiddlewareServlet();
		main.get("/:table", getAll(controller, gson));
		main.get("/:table/:id", get(controller, gson));
		main.post("/:table", create(controller, gson));
		main.use("/:table", PUT, update(controller, gson));
		main.use("/:table/:id", DELETE, delete(controller, gson));
		return main;
	}

	private static AsyncHttpServlet getAll(final Controller controller, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					String tableName = req.getUrlParameter("table");
					ReadSettings settings = ReadSettings.get(req.getParameters());
					controller.read(tableName, settings, new ResultCallback<ReadResponse>() {
						@Override
						public void onResult(ReadResponse model) {
							String json = Utils.render(gson, model);
							callback.onResult(HttpResponse.create()
									.setContentType(JSON_UTF8)
									.body(ByteBufStrings.wrapUTF8(json)));
						}

						@Override
						public void onException(Exception e) {
							callback.onResult(HttpResponse.create(500));
						}
					});
				} catch (Exception e) {
					callback.onResult(HttpResponse.create(400));
				}
			}
		};
	}

	private static AsyncHttpServlet get(final Controller controller, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					String tableName = req.getUrlParameter("table");
					ReadSettings settings = ReadSettings.get(req.getParameters());
					Integer id = Integer.valueOf(req.getUrlParameter("id"));
					controller.read(tableName, settings, id, new ResultCallback<Map<String, Object>>() {
						@Override
						public void onResult(Map<String, Object> obj) {
							String json = Utils.render(gson, obj);
							callback.onResult(HttpResponse.create()
									.setContentType(JSON_UTF8)
									.body(ByteBufStrings.wrapUTF8(json)));
						}

						@Override
						public void onException(Exception e) {
							callback.onResult(HttpResponse.create(500));
						}
					});
				} catch (NumberFormatException e) {
					callback.onResult(HttpResponse.create(400));
				}
			}
		};
	}

	private static AsyncHttpServlet create(final Controller controller, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					String tableName = req.getUrlParameter("table");
					String json = ByteBufStrings.decodeUTF8(req.getBody());
					Map<String, Object> map = Utils.parseAsObject(gson, json);
					controller.create(tableName, map, new ResultCallback<CreateResponse>() {
						@Override
						public void onResult(CreateResponse response) {
							String json = Utils.render(gson, response.toMap());
							HttpResponse res = HttpResponse.create()
									.setContentType(JSON_UTF8)
									.body(ByteBufStrings.wrapUTF8(json));
							callback.onResult(res);
						}

						@Override
						public void onException(Exception e) {
							callback.onResult(HttpResponse.create(500));
						}
					});
				} catch (Exception e) {
					callback.onResult(HttpResponse.create(400));
				}
			}
		};
	}

	private static AsyncHttpServlet update(final Controller controller, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				String json = ByteBufStrings.decodeUTF8(req.getBody());
				String tableName = req.getUrlParameter("table");
				List<List<Object>> list = Utils.parseRecordsArray(gson, json);
				controller.update(tableName, list, new ResultCallback<UpdateResponse>() {
					@Override
					public void onResult(UpdateResponse result) {
						String json = Utils.render(gson, result.toMap());
						callback.onResult(HttpResponse.create()
								.setContentType(JSON_UTF8)
								.body(ByteBufStrings.wrapUTF8(json)));
					}

					@Override
					public void onException(Exception e) {
						callback.onResult(HttpResponse.create(500));
					}
				});
			}
		};
	}

	private static AsyncHttpServlet delete(final Controller controller, final Gson gson) {
		return new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest req, final ResultCallback<HttpResponse> callback) {
				try {
					String tableName = req.getUrlParameter("table");
					Integer id = Integer.valueOf(req.getUrlParameter("id"));
					controller.delete(tableName, id, new ResultCallback<DeleteResponse>() {
						@Override
						public void onResult(DeleteResponse response) {
							HttpResponse res = HttpResponse.create();
							Map<String, Object> map = response.toMap();
							if (map != null) {
								String json = Utils.render(gson, map);
								res.setContentType(JSON_UTF8)
										.body(ByteBufStrings.wrapUTF8(json));
							}
							callback.onResult(res);
						}

						@Override
						public void onException(Exception e) {
							callback.onResult(HttpResponse.create(500));
						}
					});
				} catch (Exception e) {
					callback.onResult(HttpResponse.create(400));
				}
			}
		};
	}
}
