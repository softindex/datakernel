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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.file.File;
import io.datakernel.http.server.AsyncHttpServlet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static io.datakernel.bytebuf.ByteBuf.wrap;
import static java.nio.file.StandardOpenOption.READ;

public final class StaticServlet implements AsyncHttpServlet {
	public interface ResourceLoader {
		void getResource(String name, ResultCallback<ByteBuf> callback);
	}

	public interface CombineStrategy {
		void combine(String name, List<ResourceLoader> loaders, ResultCallback<ByteBuf> callback);
	}

	public final static String INDEX_FILE = "index.html"; // response for get request asking for root

	private final ResourceLoader resourceLoader;

	public StaticServlet(ResourceLoader resourceLoader) {
		this.resourceLoader = resourceLoader;
	}

	public StaticServlet(NioEventloop eventloop, ExecutorService executor, Path storage) {
		this.resourceLoader = new SimpleResourceLoader(eventloop, executor, storage);
	}

	@Override
	public void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
		final String trail = getTrail(request);
		resourceLoader.getResource(trail, new ResultCallback<ByteBuf>() {
			@Override
			public void onResult(ByteBuf buf) {
				callback.onResult(HttpResponse.create()
						.setContentType(defineContentType(trail))
						.body(buf));
			}

			@Override
			public void onException(Exception e) {
				callback.onResult(HttpResponse.create(404));
			}
		});
	}

	private String getTrail(HttpRequest request) {
		String trail = request.getRelativePath();
		if (request.getMethod() == HttpMethod.GET && trail.equals("/")) {
			trail = INDEX_FILE;
		} else {
			trail = trail.substring(1); // removing initial '/'
		}
		return trail;
	}

	private ContentType defineContentType(String trail) {
		int pos = trail.lastIndexOf(".");
		if (pos != -1) {
			trail = trail.substring(pos + 1);
		}
		ContentType type = ContentType.getByExt(trail);
		return type == null ? ContentType.PLAIN_TEXT : type;
	}

	public static class SimpleResourceLoader implements ResourceLoader {
		private final NioEventloop eventloop;
		private final ExecutorService executor;
		private final Path storage;

		private SimpleResourceLoader(NioEventloop eventloop, ExecutorService executor, Path storage) {
			this.eventloop = eventloop;
			this.executor = executor;
			this.storage = storage;
		}

		public static SimpleResourceLoader create(NioEventloop eventloop, ExecutorService executor, URL url) {
			Path path = Paths.get(url.getPath());
			return new SimpleResourceLoader(eventloop, executor, path);
		}

		@Override
		public void getResource(final String name, final ResultCallback<ByteBuf> callback) {
			AsyncFile.open(eventloop, executor, storage.resolve(name),
					new OpenOption[]{READ}, new ForwardingResultCallback<File>(callback) {
						@Override
						public void onResult(File file) {
							file.readFully(callback);
						}
					});
		}
	}

	public static class JarResourceLoader implements ResourceLoader {
		private final URL root;

		private JarResourceLoader(URL root) {
			this.root = root;
		}

		public static JarResourceLoader create(URL root) {
			return new JarResourceLoader(root);
		}

		@Override
		public void getResource(final String name, final ResultCallback<ByteBuf> callback) {
			try {
				URL file = new URL(root, name);
				InputStream in = file.openStream();
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				byte[] buffer = new byte[4096];
				int size;
				while ((size = in.read(buffer)) != -1) {
					out.write(buffer, 0, size);
				}
				callback.onResult(wrap(out.toByteArray()));
			} catch (IOException e) {
				callback.onException(e);
			}
		}
	}

	public static class CombinedResourceLoader implements ResourceLoader {
		private final List<ResourceLoader> providers;
		private final CombineStrategy strategy;

		private CombinedResourceLoader(List<ResourceLoader> providers, CombineStrategy strategy) {
			this.providers = providers;
			this.strategy = strategy;
		}

		public static CombinedResourceLoader create(List<ResourceLoader> providers, CombineStrategy strategy) {
			return new CombinedResourceLoader(providers, strategy);
		}

		@Override
		public void getResource(String name, ResultCallback<ByteBuf> callback) {
			strategy.combine(name, providers, callback);
		}
	}

	public static class CachedResourceLoader implements ResourceLoader {
		private final ResourceLoader provider;
		private final FileCache cache;

		private CachedResourceLoader(NioEventloop eventloop, ResourceLoader provider) {
			this.provider = provider;
			this.cache = new FileCache(eventloop);
		}

		public static CachedResourceLoader create(NioEventloop eventloop, ResourceLoader provider) {
			return new CachedResourceLoader(eventloop, provider);
		}

		@Override
		public void getResource(final String name, final ResultCallback<ByteBuf> callback) {
			cache.get(name, new ForwardingResultCallback<ByteBuf>(callback) {
				@Override
				public void onResult(ByteBuf result) {
					if (result != null) {
						callback.onResult(result);
					} else {
						provider.getResource(name, new ForwardingResultCallback<ByteBuf>(callback) {
							@Override
							public void onResult(ByteBuf result) {
								if (result != null) {
									cache.put(name, result);
								}
								callback.onResult(result);
							}
						});
					}
				}
			});
		}
	}

	private static class FileCache {
		public static final int DEFAULT_TIMEOUT = 1000;
		public static final int SYSTEM_DELAY = 100;

		private Eventloop eventloop;
		private HashMap<String, ByteBuf> entries = new HashMap<>();
		private TreeMap<Long, String> expires = new TreeMap<>();
		private int requests = 0;
		private int hits = 0;

		public FileCache(Eventloop eventloop) {
			this.eventloop = eventloop;
		}

		public void get(String name, ResultCallback<ByteBuf> callback) {
			requests++;
			ByteBuf bytes = entries.get(name);
			long time = eventloop.currentTimeMillis();
			if (bytes != null) {
				hits++;
				expires.put(time, name);
			}
			callback.onResult(bytes);
			clean(time);
		}

		public void put(String name, ByteBuf bytes) {
			long time = eventloop.currentTimeMillis();
			entries.put(name, bytes);
			expires.put(time, name);
			clean(time);
		}

		private void clean(long time) {
			long estimate = estimateTime();
			for (Iterator<Map.Entry<Long, String>> it = expires.entrySet().iterator(); it.hasNext(); ) {
				Map.Entry<Long, String> entry = it.next();
				if (time - entry.getKey() > estimate) {
					it.remove();
					entries.remove(entry.getValue());
				} else {
					return;
				}
			}
		}

		private long estimateTime() {
			return (requests - hits) * DEFAULT_TIMEOUT + SYSTEM_DELAY;
		}
	}
}