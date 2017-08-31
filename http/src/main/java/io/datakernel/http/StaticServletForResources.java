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

import io.datakernel.async.SettableStage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public final class StaticServletForResources extends StaticServlet {
	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final URL root;
	private static final byte[] ERROR_BYTES = new byte[]{};
	@SuppressWarnings("ThrowableInstanceNeverThrown")
	private static final IOException ERROR = new IOException("Resource loading error");
	private final ConcurrentHashMap<String, byte[]> cache = new ConcurrentHashMap<>();

	private StaticServletForResources(Eventloop eventloop, ExecutorService executor, URL root) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.root = root;
	}

	public static StaticServletForResources create(Eventloop eventloop, ExecutorService executor, String resourceRoot) {
		return new StaticServletForResources(eventloop, executor, ClassLoader.getSystemResource(resourceRoot));
	}

	private static byte[] loadResource(URL root, String name) throws IOException {
		URL file = new URL(root, name);

		if (!file.getPath().startsWith(root.getPath())) {
			throw new FileNotFoundException();
		}

		try (InputStream in = file.openStream()) {
			// reading file as resource
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			byte[] buffer = new byte[4096];
			int size;
			while ((size = in.read(buffer)) != -1) {
				out.write(buffer, 0, size);
			}
			return out.toByteArray();
		}
	}

	@Override
	protected final CompletionStage<ByteBuf> doServeAsync(final String name) {
		byte[] bytes = cache.get(name);
		if (bytes != null) {
			if (bytes != ERROR_BYTES) {
				return SettableStage.immediateStage(ByteBuf.wrapForReading(bytes));
			} else {
				return SettableStage.immediateFailedStage(HttpException.notFound404());
			}
		} else {
			return eventloop.callConcurrently(executor, () -> {
				try {
					byte[] bytes1 = loadResource(root, name);
					cache.put(name, bytes1);
					return ByteBuf.wrapForReading(bytes1);
				} catch (IOException e) {
					cache.put(name, ERROR_BYTES);
					if (e instanceof FileNotFoundException) {
						throw HttpException.notFound404();
					} else {
						throw e;
					}
				}
			});
		}
	}
}
