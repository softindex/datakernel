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

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;

import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

import static java.nio.file.StandardOpenOption.READ;

public final class StaticServletForFiles extends StaticServlet {
	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final Path storage;

	private StaticServletForFiles(Eventloop eventloop, ExecutorService executor, Path storage) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.storage = storage;
	}

	public static StaticServletForFiles create(Eventloop eventloop, ExecutorService executor, Path path) {
		return new StaticServletForFiles(eventloop, executor, path);
	}

	@Override
	protected final void doServeAsync(String name, final ResultCallback<ByteBuf> callback) {
		Path path = storage.resolve(name).normalize();

		if (!path.startsWith(storage)) {
			callback.setException(HttpException.notFound404());
			return;
		}

		AsyncFile.open(eventloop, executor, path,
				new OpenOption[]{READ}, new ResultCallback<AsyncFile>() {
					@Override
					public void onResult(AsyncFile file) {
						file.readFully(callback);
					}

					@Override
					public void onException(Exception exception) {
						if (exception instanceof NoSuchFileException)
							callback.setException(HttpException.notFound404());
						else
							callback.setException(exception);
					}
				});
	}
}
