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

package io.datakernel.http.middleware;

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.file.AsyncFileSystem;
import io.datakernel.file.File;
import io.datakernel.file.FileSystem;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.time.CurrentTimeProviderSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.io.Files.getFileExtension;
import static io.datakernel.http.ContentTypes.extensionContentType;
import static io.datakernel.http.HttpHeader.*;
import static java.nio.file.StandardOpenOption.READ;
import static java.util.concurrent.Executors.newCachedThreadPool;

public final class MiddlewareStaticServer {
	private static final Logger logger = LoggerFactory.getLogger(MiddlewareStaticServer.class);

	public static HttpSuccessHandler serveStatic(final String directory) {
		NioEventloop eventloop = new NioEventloop(new ByteBufPool(32, 1 << 25), CurrentTimeProviderSystem.instance());
		return serveStatic(directory, eventloop);
	}

	public static HttpSuccessHandler serveStatic(final String directory, final NioEventloop eventloop) {
		return serveStatic(directory, eventloop, new AsyncFileSystem(eventloop, newCachedThreadPool()));
	}

	private static ByteBuf getContentType(String path) {
		String fileExtension = getFileExtension(path);

		ByteBuf contentType = extensionContentType.get(fileExtension);

		return contentType;
	}

	public static HttpSuccessHandler serveStatic(final String directory, final NioEventloop eventloop,
	                                             final FileSystem fileSystem) {
		return new HttpSuccessHandler() {
			@Override
			public void handle(final HttpRequest request, final MiddlewareRequestContext context) {
				final String urlTrail = context.getUrlTrail();
				final String filePath = directory + "/" + urlTrail;
				logger.info("Got request. File path: {}", filePath);
				Path path = Paths.get(filePath);
				final HttpResponse response = HttpResponse.create();

				fileSystem.open(path, new OpenOption[]{READ}, new ResultCallback<File>() {
					@Override
					public void onResult(File file) {
						file.readFully(new ResultCallback<ByteBuf>() {
							@Override
							public void onResult(ByteBuf byteBuf) {
								response.header(CONTENT_LENGTH, valueOfDecimal(byteBuf.limit()));

								ByteBuf contentType = getContentType(urlTrail);
								if (contentType != null) {
									response.header(CONTENT_TYPE, contentType);
								}

								response.body(byteBuf);
								context.send(response);
								logger.info("Sent response. File path: {}", filePath);

								byteBuf.recycle();
							}

							@Override
							public void onException(Exception exception) {
								logger.info("Could not read file.", exception);
								context.next(request);
							}
						});
					}

					@Override
					public void onException(Exception exception) {
						logger.info("Could not open file.", exception);
						context.next(request);
					}
				});

				eventloop.run();
			}
		};
	}
}
