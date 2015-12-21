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
import io.datakernel.http.StaticServlet.ResourceLoader;
import io.datakernel.http.StaticServlet.StaticResource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

import static io.datakernel.bytebuf.ByteBuf.wrap;
import static java.nio.file.StandardOpenOption.READ;

public class ResourceLoaders {
	public static final class SimpleResourceLoader extends ResourceLoader {
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
		public void getResource(HttpRequest request, final ResultCallback<StaticResource> callback) {
			final String name = getTrail(request);
			AsyncFile.open(eventloop, executor, storage.resolve(name),
					new OpenOption[]{READ}, new ForwardingResultCallback<File>(callback) {
						@Override
						public void onResult(File file) {
							file.readFully(new ForwardingResultCallback<ByteBuf>(callback) {
								@Override
								public void onResult(ByteBuf buf) {
									ContentType type = defineContentType(name);
									if (ContentType.isText(type)) {
										type = type.setCharsetEncoding(DEFAULT_TXT_ENCODING);
									}
									callback.onResult(new StaticResource(buf, type));
								}
							});
						}
					});
		}
	}

	public static final class JarResourceLoader extends ResourceLoader {
		private final URL root;

		private JarResourceLoader(URL root) {
			this.root = root;
		}

		public static JarResourceLoader create(URL root) {
			return new JarResourceLoader(root);
		}

		@Override
		public void getResource(HttpRequest request, final ResultCallback<StaticResource> callback) {
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

				// specifying default encoding for txt files
				ContentType type = defineContentType(name);
				if (ContentType.isText(type)) {
					type.setCharsetEncoding(DEFAULT_TXT_ENCODING);
				}

				callback.onResult(new StaticResource(wrap(out.toByteArray()), type));
			} catch (IOException e) {
				callback.onException(e);
			}
		}
	}

	private static final class CachedResourceLoader extends ResourceLoader {
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
		public void getResource(final HttpRequest request, final ResultCallback<StaticResource> callback) {
			final String name = getTrail(request);
			cache.get(name, new ForwardingResultCallback<StaticResource>(callback) {
				@Override
				public void onResult(StaticResource result) {
					if (result != null) {
						callback.onResult(result);
					} else {
						provider.getResource(request, new ForwardingResultCallback<StaticResource>(callback) {
							@Override
							public void onResult(StaticResource result) {
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
		private HashMap<String, StaticResource> entries = new HashMap<>();
		private TreeMap<Long, String> expires = new TreeMap<>();
		private int requests = 0;
		private int hits = 0;

		public FileCache(Eventloop eventloop) {
			this.eventloop = eventloop;
		}

		public void get(String name, ResultCallback<StaticResource> callback) {
			requests++;
			StaticResource resource = entries.get(name);
			long time = eventloop.currentTimeMillis();
			if (resource != null) {
				hits++;
				expires.put(time, name);
			}
			callback.onResult(resource);
			clean(time);
		}

		public void put(String name, StaticResource bytes) {
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

	// builders
	public static ResourceLoader line(final ResourceLoader... loaders) {
		return new ResourceLoader() {
			@Override
			protected void getResource(HttpRequest request, ResultCallback<StaticResource> callback) {
				int current = 0;
				getResource(current, request, callback);
			}

			private void getResource(final int current, final HttpRequest request, final ResultCallback<StaticResource> callback) {
				loaders[current].getResource(request, new ResultCallback<StaticResource>() {
					@Override
					public void onResult(StaticResource result) {
						callback.onResult(result);
					}

					@Override
					public void onException(Exception e) {
						if (current < loaders.length - 1) {
							getResource(current + 1, request, callback);
						} else {
							callback.onException(e);
						}
					}
				});
			}
		};
	}

	public static ResourceLoader cached(NioEventloop eventloop, ResourceLoader loader) {
		return new CachedResourceLoader(eventloop, loader);
	}
}
