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
import io.datakernel.http.server.AsyncHttpServlet;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

import static java.nio.file.StandardOpenOption.READ;

public class StaticServlet implements AsyncHttpServlet {
	private final String directory;
	private final NioEventloop eventloop;
	private final ExecutorService executor;

	public StaticServlet(String directory, NioEventloop eventloop, ExecutorService executor) {
		this.directory = directory;
		this.eventloop = eventloop;
		this.executor = executor;
	}

	@Override
	public void serveAsync(HttpRequest request, final ResultCallback<HttpResponse> callback) {
		final String urlTrail = request.getRelativePath();
		Path path = Paths.get(directory + urlTrail);

		findFile(path, new ResultCallback<ByteBuf>() {
			@Override
			public void onResult(ByteBuf buf) {
				callback.onResult(HttpResponse.create()
						.setContentType(defineContentType(urlTrail))
						.body(buf));
			}

			@Override
			public void onException(Exception e) {
				callback.onResult(HttpResponse.create(404));
			}
		});
	}

	private void findFile(Path path, final ResultCallback<ByteBuf> callback) {
		AsyncFile.open(eventloop, executor, path, new OpenOption[]{READ}, new ForwardingResultCallback<File>(callback) {
			@Override
			public void onResult(final File file) {
				file.readFully(callback);
			}
		});
	}

	private ContentType defineContentType(String trail) {
		int pos = trail.lastIndexOf(".");
		if (pos != -1) {
			trail = trail.substring(pos + 1);
		}
		ContentType type = ContentType.getByExt(trail);
		return type == null ? ContentType.PLAIN_TEXT : type;
	}
}