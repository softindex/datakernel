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

import com.google.common.collect.Lists;
import io.datakernel.async.ResultCallback;
import io.datakernel.http.HttpMethod;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.server.AsyncHttpServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;

public final class MiddlewareServlet implements AsyncHttpServlet {
	private static final Logger logger = LoggerFactory.getLogger(MiddlewareServlet.class);

	private final List<HttpRequestSuccessHandler> successHandlers = Lists.newArrayList();
	private final List<HttpErrorHandler> errorHandlers = Lists.newArrayList();

	private static final String ONE_OR_MORE_OF_ANY_CHARACTERS_EXCEPT_SLASH = "[^/]+";
	private static final String ANY_NUMBER_OF_PARAMS_CAPTURING = "((?:[^/]+/?)*)";
	private static final String ANY_NUMBER_OF_ANY_CHARACTERS = ".*";
	private static final String SLASH = "/";
	private static final String COLON = ":";
	private static final String QUESTION_MARK = "?";

	public void use(HttpSuccessHandler handler) {
		addSuccessHandler(SLASH + ANY_NUMBER_OF_ANY_CHARACTERS, handler);
	}

	public void use(HttpErrorHandler handler) {
		addErrorHandler(handler);
	}

	public void use(String path, HttpSuccessHandler handler) {
		path = preprocessPath(path);

		if (path.contains(COLON)) {
			addSuccessHandlerWithNamedGroups(path, handler);
			return;
		}

		if (path.equals(SLASH)) {
			path += ANY_NUMBER_OF_ANY_CHARACTERS;
		} else {
			path += ANY_NUMBER_OF_PARAMS_CAPTURING;
		}

		addSuccessHandler(path, handler);
	}

	public void use(Pattern pattern, HttpSuccessHandler handler) {
		addSuccessHandler(pattern, handler);
	}

	public void use(HttpMethod method, String path, HttpSuccessHandler handler) {
		path = preprocessPath(path);

		if (path.contains(COLON)) {
			addSuccessHandlerWithNamedGroups(path, handler);
			return;
		}

		addSuccessHandler(path, method, handler);
	}

	public void use(HttpMethod method, Pattern pattern, HttpSuccessHandler handler) {
		addSuccessHandler(pattern, method, handler);
	}

	public void get(String path, HttpSuccessHandler handler) {
		use(HttpMethod.GET, path, handler);
	}

	public void get(Pattern pattern, HttpSuccessHandler handler) {
		use(HttpMethod.GET, pattern, handler);
	}

	public void post(String path, HttpSuccessHandler handler) {
		use(HttpMethod.POST, path, handler);
	}

	public void post(Pattern pattern, HttpSuccessHandler handler) {
		use(HttpMethod.POST, pattern, handler);
	}

	private void addSuccessHandler(String path, HttpSuccessHandler handler) {
		addSuccessHandler(path, null, handler);
	}

	private void addSuccessHandler(Pattern pattern, HttpSuccessHandler handler) {
		addSuccessHandler(pattern, null, handler);
	}

	private void addSuccessHandlerWithNamedGroups(String path, HttpSuccessHandler handler) {
		addSuccessHandlerWithNamedGroups(path, null, handler);
	}

	private void addSuccessHandlerWithNamedGroups(String path, HttpMethod method, HttpSuccessHandler handler) {
		List<String> namedGroups = new ArrayList<>();

		String[] urlComponents = path.split(SLASH);
		for (String urlComponent : urlComponents) {
			if (urlComponent.startsWith(COLON)) {
				boolean optional = urlComponent.endsWith(QUESTION_MARK);
				String groupName;
				if (optional) {
					groupName = urlComponent.substring(1, urlComponent.length() - 1);
					path = path.replace("/" + urlComponent,
							"(" + SLASH + "(?<" + groupName + ">" + ONE_OR_MORE_OF_ANY_CHARACTERS_EXCEPT_SLASH + "))" + "?");
				} else {
					groupName = urlComponent.substring(1);
					path = path.replace(urlComponent,
							"(?<" + groupName + ">" + ONE_OR_MORE_OF_ANY_CHARACTERS_EXCEPT_SLASH + ")");
				}
				namedGroups.add(groupName);
			}
		}

		addSuccessHandler(compile(path), method, handler, namedGroups);
	}

	private void addSuccessHandler(Pattern pattern, HttpMethod method, HttpSuccessHandler handler) {
		addSuccessHandler(pattern, method, handler, new ArrayList<String>());
	}

	private void addSuccessHandler(String path, HttpMethod method, HttpSuccessHandler handler) {
		addSuccessHandler(compile(path), method, handler, new ArrayList<String>());
	}

	private void addSuccessHandler(Pattern pattern, HttpMethod method, HttpSuccessHandler handler, List<String> namedGroups) {
		successHandlers.add(new HttpRequestSuccessHandler(handler, method, pattern, namedGroups));
	}

	private void addErrorHandler(HttpErrorHandler handler) {
		errorHandlers.add(handler);
	}

	private String preprocessPath(String path) {
		return getPathWithTrailingSlash(path.toLowerCase());
	}

	private String getPathWithTrailingSlash(String path) {
		String pathWithTrailingSlash;

		if (path != null && path.length() > 1 && !path.endsWith("/")) {
			pathWithTrailingSlash = path + "/";
		} else {
			pathWithTrailingSlash = path;
		}

		return pathWithTrailingSlash;
	}

	private boolean isHandlerMatching(HttpRequest request, HttpRequestSuccessHandler requestHandler) {
		Pattern handlerPattern = requestHandler.getPattern();
		String requestPath = request.getPath();
		requestPath = preprocessPath(requestPath);
		HttpMethod handlerMethod = requestHandler.getMethod();
		HttpMethod requestMethod = request.getMethod();

		boolean wildcardMethod = handlerMethod == null;
		boolean pathsMatch = handlerPattern.matcher(requestPath).matches();

		if (pathsMatch && wildcardMethod) {
			return true;
		}

		boolean methodsMatch = requestMethod.equals(handlerMethod);

		if (pathsMatch && methodsMatch) {
			return true;
		}

		return false;
	}

	private MiddlewareRequestContext createMiddlewareRequestContext(final Iterator<HttpRequestSuccessHandler> successHandlersIterator,
	                                                                final Iterator<HttpErrorHandler> errorHandlersIterator, String urlTrail,
	                                                                Map<String, String> urlParameters, final Map<Object, Object> attachment,
	                                                                final ResultCallback<HttpResponse> callback) {
		return new MiddlewareRequestContext(urlParameters, urlTrail, attachment) {
			@Override
			public void next(HttpRequest request) {
				callNextSuccessHandler(successHandlersIterator, errorHandlersIterator,
						request, attachment, callback);
			}

			@Override
			public void next(Exception exception, HttpRequest request) {
				callNextErrorHandler(errorHandlersIterator, exception, request, attachment, callback);
			}

			@Override
			public void send(HttpResponse response) {
				callback.onResult(response);
			}
		};
	}

	void callNextSuccessHandler(Iterator<HttpRequestSuccessHandler> successHandlersIterator,
	                            Iterator<HttpErrorHandler> errorHandlersIterator,
	                            HttpRequest request, Map<Object, Object> attachment,
	                            ResultCallback<HttpResponse> callback) {
		HttpRequestSuccessHandler nextSuccessHandler = findNextSuccessHandler(request, successHandlersIterator);

		if (nextSuccessHandler != null) {
			try {
				List<String> handlerGroupNames = nextSuccessHandler.getGroupNames();
				Matcher matcher = nextSuccessHandler.getPattern().matcher(preprocessPath(request.getPath()));
				Map<String, String> urlParameters;

				if (handlerGroupNames.size() > 0) {
					urlParameters = getUrlParameters(handlerGroupNames, matcher);
				} else {
					urlParameters = new LinkedHashMap<>();
				}

				String urlTrail = getUrlTrail(matcher);

				nextSuccessHandler.getHandler().handle(request,
						createMiddlewareRequestContext(successHandlersIterator, errorHandlersIterator, urlTrail, urlParameters, attachment, callback));
			} catch (RuntimeException runtimeException) {
				logger.debug("Runtime exception thrown while executing success handlers.", runtimeException);
				callNextErrorHandler(errorHandlersIterator, runtimeException, request, attachment, callback);
			}
		} else {
			callback.onResult(HttpResponse.notFound404());
		}
	}

	private Map<String, String> getUrlParameters(List<String> groupNames, Matcher matcher) {
		Map<String, String> urlParameters = new LinkedHashMap<>();

		if (matcher.matches()) {
			for (String groupName : groupNames) {
				String capturedGroup = matcher.group(groupName);
				if (capturedGroup != null && capturedGroup.length() > 0) {
					urlParameters.put(groupName, capturedGroup);
				}
			}
		}

		return urlParameters;
	}

	private String getUrlTrail(Matcher matcher) {
		if (matcher.matches()) {
			int groupCount = matcher.groupCount();
			if (groupCount == 0) {
				return null;
			}

			return matcher.group(groupCount);
		}

		return null;
	}

	void callNextErrorHandler(Iterator<HttpErrorHandler> errorHandlersIterator, Exception exception,
	                          HttpRequest request, Map<Object, Object> attachment, ResultCallback<HttpResponse> callback) {
		if (!errorHandlersIterator.hasNext()) {
			callback.onResult(HttpResponse.internalServerError500());
			return;
		}

		HttpErrorHandler nextErrorHandler = errorHandlersIterator.next();

		try {
			nextErrorHandler.handle(exception, request, createMiddlewareRequestErrorContext(errorHandlersIterator, attachment, callback));
		} catch (RuntimeException runtimeException) {
			logger.debug("Runtime exception thrown while executing error handlers.", runtimeException);
			callNextErrorHandler(errorHandlersIterator, runtimeException, request, attachment, callback);
		}
	}

	private MiddlewareRequestErrorContext createMiddlewareRequestErrorContext(final Iterator<HttpErrorHandler> errorHandlersIterator,
	                                                                          final Map<Object, Object> attachment,
	                                                                          final ResultCallback<HttpResponse> callback) {
		return new MiddlewareRequestErrorContext(attachment) {
			@Override
			public void next(Exception exception, HttpRequest request) {
				callNextErrorHandler(errorHandlersIterator, exception, request, attachment, callback);
			}

			@Override
			public void send(HttpResponse response) {
				callback.onResult(response);
			}
		};
	}

	private HttpRequestSuccessHandler findNextSuccessHandler(HttpRequest request, Iterator<HttpRequestSuccessHandler> iterator) {
		while (iterator.hasNext()) {
			HttpRequestSuccessHandler handler = iterator.next();
			boolean matching = isHandlerMatching(request, handler);
			if (matching) {
				return handler;
			}
		}

		return null;
	}

	@Override
	public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
		Iterator<HttpRequestSuccessHandler> successHandlersIterator = successHandlers.iterator();

		Iterator<HttpErrorHandler> errorHandlersIterator = errorHandlers.iterator();

		Map<Object, Object> attachment = new HashMap<>();
		callNextSuccessHandler(successHandlersIterator, errorHandlersIterator,
				request, attachment, callback);
	}
}
