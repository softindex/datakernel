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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.callConcurrently;

public final class StaticServletForResources extends StaticServlet {
	private final NioEventloop eventloop;
	private final ExecutorService executor;
	private final URL root;
	private static final byte[] ERROR_BYTES = new byte[]{};
	@SuppressWarnings("ThrowableInstanceNeverThrown")
	private static final IOException ERROR = new IOException("Resource loading error");
	private final ConcurrentHashMap<String, byte[]> cache = new ConcurrentHashMap<>();

	public StaticServletForResources(NioEventloop eventloop, ExecutorService executor, String resourceRoot) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.root = ClassLoader.getSystemResource(resourceRoot);
	}

	private static byte[] loadResource(URL root, String name) throws IOException {
		URL file = new URL(root, name);
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
	protected final void doServeAsync(final String name, final ForwardingResultCallback<ByteBuf> callback) {
		byte[] bytes = cache.get(name);
		if (bytes != null) {
			if (bytes == ERROR_BYTES) {
				callback.onException(ERROR);
			} else {
				callback.onResult(ByteBuf.wrap(bytes));
			}
		} else {
			callConcurrently(eventloop, executor, false, new Callable<ByteBuf>() {
				@Override
				public ByteBuf call() throws Exception {
					try {
						byte[] bytes = loadResource(root, name);
						cache.put(name, bytes);
						return ByteBuf.wrap(bytes);
					} catch (Exception e) {
						cache.put(name, ERROR_BYTES);
						throw e;
					}
				}
			}, callback);
		}
	}
}
