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
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.loader.StaticLoader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;

public final class StaticServlet implements AsyncServlet {
	public static final Charset DEFAULT_TXT_ENCODING = StandardCharsets.UTF_8;

	private final StaticLoader resourceLoader;
	private Function<String, ContentType> contentTypeResolver = StaticServlet::getContentType;
	private Function<HttpRequest, @Nullable String> mapper = HttpRequest::getRelativePath;
	private Set<String> indexResources = new LinkedHashSet<>();

	@Nullable
	private String defaultResource;

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

	public StaticServlet withMapping(Function<HttpRequest, String> fn) {
		mapper = fn;
		return this;
	}

	public StaticServlet withMappingTo(String path) {
		if (this.contentTypeResolver == (Function<String, ContentType>) StaticServlet::getContentType) {
			withContentType(getContentType(path));
		}
		return withMapping($ -> path);
	}

	public StaticServlet withMappingNotFoundTo(String defaultResource) {
		this.defaultResource = defaultResource;
		return this;
	}

	public StaticServlet withIndexResources(String... indexResources) {
		this.indexResources.addAll(Arrays.asList(indexResources));
		return this;
	}

	public StaticServlet withIndexHtml() {
		this.indexResources.add("index.html");
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

	private HttpResponse createHttpResponse(ByteBuf buf, ContentType contentType) {
		return HttpResponse.ofCode(200)
				.withBody(buf)
				.withHeader(CONTENT_TYPE, ofContentType(contentType));
	}

	@NotNull
	@Override
	public final Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		String mappedPath = mapper.apply(request);
		if (mappedPath == null) return Promise.ofException(HttpException.notFound404());
		ContentType contentType = contentTypeResolver.apply(mappedPath);
		return Promise.complete()
				.then($ -> (mappedPath.endsWith("/") || mappedPath.isEmpty()) ?
						tryLoadIndexResource(mappedPath) :
						resourceLoader.load(mappedPath)
								.map(byteBuf -> createHttpResponse(byteBuf, contentType))
								.thenEx((value, e) -> {
									if (e == StaticLoader.IS_A_DIRECTORY) {
										return tryLoadIndexResource(mappedPath);
									} else {
										return Promise.of(value, e);
									}
								}))
				.thenEx((response, e) -> {
					if (e == null) {
						return Promise.of(response);
					} else if (e == StaticLoader.NOT_FOUND_EXCEPTION) {
						return tryLoadDefaultResource();
					} else {
						return Promise.ofException(e);
					}
				});
	}

	@NotNull
	private Promise<HttpResponse> tryLoadIndexResource(String mappedPath) {
		String dirPath = mappedPath.endsWith("/") || mappedPath.isEmpty() ? mappedPath : (mappedPath + '/');
		return Promises.<HttpResponse>firstSuccessful(indexResources.stream()
				.map(indexResource -> () -> resourceLoader.load(dirPath + indexResource)
						.map(byteBuf -> createHttpResponse(byteBuf, contentTypeResolver.apply(indexResource)))))
				.thenEx(((response, e) -> e == null ? Promise.of(response) : Promise.ofException(StaticLoader.NOT_FOUND_EXCEPTION)));
	}

	@NotNull
	private Promise<? extends HttpResponse> tryLoadDefaultResource() {
		return defaultResource != null ?
				resourceLoader.load(defaultResource)
						.map(buf -> createHttpResponse(buf, contentTypeResolver.apply(defaultResource))) :
				Promise.ofException(HttpException.notFound404());
	}
}
