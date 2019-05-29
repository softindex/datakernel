/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.async.Promise;
import io.datakernel.loader.StaticLoader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;

public final class StaticServlet implements AsyncServlet {
	public static final Charset DEFAULT_TXT_ENCODING = StandardCharsets.UTF_8;

	private final StaticLoader resourceLoader;
	private Function<String, ContentType> contentTypeResolver = StaticServlet::getContentType;
	private Function<String, @Nullable String> mapper = path -> path;
	private Predicate<String> filter = path -> true;

	private StaticServlet(StaticLoader resourceLoader) {
		this.resourceLoader = resourceLoader;
	}

	public static StaticServlet create(StaticLoader resourceLoader) {
		return new StaticServlet(resourceLoader);
	}

	public static StaticServlet create(Path path) {
		return new StaticServlet(StaticLoader.ofPath(path));
	}

	public StaticServlet withContentType(ContentType contentType) {
		return withContentTypeResolver($ -> contentType);
	}

	public StaticServlet withContentTypeResolver(Function<String, ContentType> contentTypeResolver) {
		this.contentTypeResolver = contentTypeResolver;
		return this;
	}

	public StaticServlet withMapping(Function<String, String> fn) {
		mapper = fn;
		return this;
	}

	public StaticServlet withMappingTo(String path) {
		if (this.contentTypeResolver == (Function<String, ContentType>) StaticServlet::getContentType) {
			withContentType(getContentType(path));
		}
		return withMapping($ -> path);
	}

	public StaticServlet withMappingEmptyTo(String index) {
		return withMapping(filename -> filename.isEmpty() ? index : filename);
	}

	public StaticServlet withFilter(Predicate<String> filter) {
		this.filter = filter;
		return this;
	}

	public static ContentType getContentType(String path) {
		int pos = path.lastIndexOf(".");
		if (pos == -1) {
			return ContentType.of(MediaTypes.OCTET_STREAM);
		}

		String ext = path.substring(pos + 1);

		MediaType mime = MediaTypes.getByExtension(ext);
		if (mime == null) {
			mime = MediaTypes.OCTET_STREAM;
		}

		ContentType type;
		if (mime.isTextType()) {
			type = ContentType.of(mime, DEFAULT_TXT_ENCODING);
		} else {
			type = ContentType.of(mime);
		}

		return type;
	}

	@NotNull
	@Override
	public final Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		String relativePath = request.getRelativePath();
		String mappedPath = mapper.apply(relativePath);
		if (!filter.test(relativePath) || mappedPath == null) return Promise.ofException(HttpException.notFound404());
		ContentType contentType = contentTypeResolver.apply(mappedPath);

		return resourceLoader.load(mappedPath)
				.thenEx((byteBuf, e) -> {
					if (e == null) {
						return Promise.of(
								HttpResponse.ofCode(200)
										.withBody(byteBuf)
										.withHeader(CONTENT_TYPE, ofContentType(contentType)));
					} else {
						return e == StaticLoader.NOT_FOUND_EXCEPTION ?
								Promise.ofException(HttpException.notFound404()) :
								Promise.ofException(e);
					}
				});
	}
}
