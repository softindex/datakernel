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

package io.datakernel.http;

import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.file.File;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

import static io.datakernel.bytebuf.ByteBuf.wrap;
import static java.nio.file.StandardOpenOption.READ;

public final class StaticServlets {
	public static final class BaseStaticServlet extends StaticServlet {
		private final NioEventloop eventloop;
		private final ExecutorService executor;
		private final Path storage;

		private BaseStaticServlet(NioEventloop eventloop, ExecutorService executor, Path storage) {
			this.eventloop = eventloop;
			this.executor = executor;
			this.storage = storage;
		}

		public static BaseStaticServlet create(NioEventloop eventloop, ExecutorService executor, URL url) {
			Path path = Paths.get(url.getPath());
			return new BaseStaticServlet(eventloop, executor, path);
		}

		@Override
		public void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
			final String name = getTrail(request);
			AsyncFile.open(eventloop, executor, storage.resolve(name),
					new OpenOption[]{READ}, new ForwardingResultCallback<File>(callback) {
						@Override
						public void onResult(File file) {
							file.readFully(new ForwardingResultCallback<ByteBuf>(callback) {
								@Override
								public void onResult(ByteBuf buf) {
									ContentType type = defineContentType(name);
									callback.onResult(HttpResponse.create(200)
											.body(buf)
											.setContentType(type));
								}
							});
						}
					});
		}
	}

	public static final class JarStaticServlet extends StaticServlet {
		private final URL root;

		private JarStaticServlet(URL root) {
			this.root = root;
		}

		public static JarStaticServlet create(URL root) {
			return new JarStaticServlet(root);
		}

		@Override
		public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
			final String name = getTrail(request);
			try {
				// reading file as resource
				URL file = new URL(root, name);
				InputStream in = file.openStream();
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				byte[] buffer = new byte[4096];
				int size;
				while ((size = in.read(buffer)) != -1) {
					out.write(buffer, 0, size);
				}
				// specifying content type
				ContentType type = defineContentType(name);
				ByteBuf buf = wrap(out.toByteArray());
				callback.onResult(HttpResponse.create().body(buf).setContentType(type));
			} catch (Exception e) {
				callback.onException(e);
			}
		}
	}

	public static StaticServlet getLinear(final Iterable<StaticServlet> servlets) {
		return new StaticServlet() {
			@Override
			public void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
				Iterator<StaticServlet> iterator = servlets.iterator();
				if (iterator.hasNext()) {
					serve(iterator, request, callback);
				}
			}

			private void serve(final Iterator<StaticServlet> iterator, final HttpRequest request, final ResultCallback<HttpResponse> callback) {
				iterator.next().serveAsync(request, new ResultCallback<HttpResponse>() {
					@Override
					public void onResult(HttpResponse result) {
						callback.onResult(result);
					}

					@Override
					public void onException(Exception e) {
						if (iterator.hasNext()) {
							serve(iterator, request, callback);
						} else {
							callback.onResult(HttpResponse.notFound404());
						}
					}
				});
			}
		};
	}
}
