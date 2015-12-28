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
import io.datakernel.file.AsyncFile;
import io.datakernel.file.File;

import java.net.URL;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

import static java.nio.file.StandardOpenOption.READ;

public final class StaticServletForFiles extends StaticServlet {
	private final NioEventloop eventloop;
	private final ExecutorService executor;
	private final Path storage;

	private StaticServletForFiles(NioEventloop eventloop, ExecutorService executor, Path storage) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.storage = storage;
	}

	public static StaticServletForFiles create(NioEventloop eventloop, ExecutorService executor, URL url) {
		Path path = Paths.get(url.getPath());
		return new StaticServletForFiles(eventloop, executor, path);
	}

	@Override
	protected final void doServeAsync(String name, final ForwardingResultCallback<ByteBuf> callback) {
		AsyncFile.open(eventloop, executor, storage.resolve(name),
				new OpenOption[]{READ}, new ForwardingResultCallback<File>(callback) {
					@Override
					public void onResult(File file) {
						file.readFully(callback);
					}
				});
	}
}
