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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class MiddlewareServlet implements AsyncHttpServlet {
	private static final String ROOT = "/";

	protected final Map<String, MiddlewareServlet> routes = new HashMap<>();
	protected AsyncHttpServlet fallbackRoute;

	protected final Map<HttpMethod, AsyncHttpServlet> handlers = new HashMap<>();
	protected AsyncHttpServlet fallbackHandler;

	protected final Map<String, MiddlewareServlet> parameters = new HashMap<>();

	public void get(AsyncHttpServlet handler) {
		get(ROOT, handler);
	}

	public void get(String path, AsyncHttpServlet handler) {
		use(path, HttpMethod.GET, handler);
	}

	public void post(AsyncHttpServlet handler) {
		post(ROOT, handler);
	}

	public void post(String path, AsyncHttpServlet handler) {
		use(path, HttpMethod.POST, handler);
	}

	public void use(AsyncHttpServlet handler) {
		use(ROOT, null, handler);
	}

	public void use(HttpMethod method, AsyncHttpServlet handler) {
		use(ROOT, method, handler);
	}

	public void use(String path, AsyncHttpServlet handler) {
		use(path, null, handler);
	}

	public void use(String path, HttpMethod method, AsyncHttpServlet handler) {
		if (!path.isEmpty() && !path.startsWith(ROOT))
			throw new RuntimeException("Bad path. Should start with \"/\"");
		if (path.isEmpty() || path.equals(ROOT)) {
			apply(method, handler);
		} else {
			int slash = path.indexOf('/', 1);
			String remainingPath;
			String urlPart;
			if (slash == -1) {
				remainingPath = "";
				urlPart = path.substring(1);
			} else {
				remainingPath = path.substring(slash);
				urlPart = path.substring(1, slash);
			}
			MiddlewareServlet container = ensureMServlet(urlPart);
			container.use(remainingPath, method, handler);
		}
	}

	public void setDefault(AsyncHttpServlet fallback) {
		this.fallbackRoute = fallback;
	}

	public void setDefault(String path, AsyncHttpServlet fallback) {
		if (!path.isEmpty() && !path.startsWith(ROOT))
			throw new RuntimeException("Bad path. Should start with \"/\".");
		if (path.isEmpty() || path.equals(ROOT)) {
			setDefault(fallback);
		} else {
			int slash = path.indexOf('/', 1);
			String remainingPath;
			String urlPart;
			if (slash == -1) {
				remainingPath = "";
				urlPart = path.substring(1);
			} else {
				remainingPath = path.substring(slash);
				urlPart = path.substring(1, slash);
			}
			MiddlewareServlet container = ensureMServlet(urlPart);
			container.setDefault(remainingPath, fallback);
		}
	}

	@Override
	public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
		boolean processed = tryServeAsync(request, callback);
		if (!processed) {
			callback.onResult(HttpResponse.notFound404());
		}
	}

	protected boolean tryServeAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
		int introPosition = request.getPos();
		String urlPart = request.pollUrlPart();
		HttpMethod method = request.getMethod();

		if (urlPart.isEmpty()) {
			AsyncHttpServlet handler = getHandlerOrWildcard(method);
			if (handler != null) {
				handler.serveAsync(request, callback);
				return true;
			}
		}

		boolean processed = false;

		MiddlewareServlet transit = routes.get(urlPart);
		if (transit != null) {
			processed = transit.tryServeAsync(request, callback);
		} else {
			int position = request.getPos();
			for (Entry<String, MiddlewareServlet> entry : parameters.entrySet()) {
				request.putUrlParameter(entry.getKey(), urlPart);
				processed = entry.getValue().tryServeAsync(request, callback);
				if (processed) {
					return true;
				} else {
					request.removeUrlParameter(entry.getKey());
					request.setPos(position);
				}
			}
		}

		if (!processed && fallbackRoute != null) {
			request.setPos(introPosition);
			fallbackRoute.serveAsync(request, callback);
			processed = true;
		}
		return processed;
	}

	private AsyncHttpServlet getHandlerOrWildcard(HttpMethod method) {
		AsyncHttpServlet handler = handlers.get(method);
		if (handler == null) {
			return fallbackHandler;
		}
		return handler;
	}

	private void apply(HttpMethod method, AsyncHttpServlet handler) {
		if (handler instanceof MiddlewareServlet) {
			merge(this, (MiddlewareServlet) handler);
		} else if (method == null && fallbackHandler == null) {
			fallbackHandler = handler;
		} else if (handlers.get(method) == null) {
			handlers.put(method, handler);
		} else if (this.handlers.get(method) != handler) {
			throw new RuntimeException("Can't map. Handler already exists");
		}
	}

	private void merge(MiddlewareServlet mServlet1, MiddlewareServlet mServlet2) {

		if (mServlet1 == mServlet2) {
			return;
		}

		for (Entry<String, MiddlewareServlet> entry : mServlet2.routes.entrySet()) {
			String key = entry.getKey();
			MiddlewareServlet ms1 = mServlet1.routes.get(key);
			MiddlewareServlet ms2 = entry.getValue();
			if (ms1 != null) {
				merge(ms1, ms2);
			} else {
				mServlet1.routes.put(key, ms2);
			}
		}

		for (Entry<String, MiddlewareServlet> entry : mServlet2.parameters.entrySet()) {
			String name = entry.getKey();
			MiddlewareServlet ps1 = mServlet1.parameters.get(name);
			MiddlewareServlet ps2 = entry.getValue();
			if (ps1 != null) {
				merge(ps1, ps2);
			} else {
				mServlet1.parameters.put(name, ps2);
			}
		}

		for (Entry<HttpMethod, AsyncHttpServlet> entry : mServlet2.handlers.entrySet()) {
			HttpMethod key = entry.getKey();
			AsyncHttpServlet s1 = mServlet1.handlers.get(key);
			AsyncHttpServlet s2 = entry.getValue();
			if (s1 == null) {
				mServlet1.handlers.put(key, s2);
			} else if (s1 != s2) {
				throw new RuntimeException("Can't map. Handler for this method already exists");
			}
		}

		if (mServlet1.fallbackHandler == null) {
			mServlet1.fallbackHandler = mServlet2.fallbackHandler;
		} else if (mServlet2.fallbackHandler != null && mServlet1.fallbackHandler != mServlet2.fallbackHandler) {
			throw new RuntimeException("Can't map. Handler for this method already exists");
		}

		if (mServlet1.fallbackRoute == null) {
			mServlet1.fallbackRoute = mServlet2.fallbackRoute;
		} else if (mServlet2.fallbackRoute != null && mServlet1.fallbackRoute != mServlet2.fallbackRoute) {
			throw new RuntimeException("Can't map. Fallback already exists.");
		}

	}

	private MiddlewareServlet ensureMServlet(String urlPart) {
		MiddlewareServlet servlet;
		if (urlPart.startsWith(":")) {
			urlPart = urlPart.substring(1);
			MiddlewareServlet parameter = parameters.get(urlPart);
			if (parameter == null) {
				servlet = new MiddlewareServlet();
				parameters.put(urlPart, servlet);
			} else {
				return parameter;
			}
		} else {
			MiddlewareServlet container = routes.get(urlPart);
			if (container == null) {
				servlet = new MiddlewareServlet();
				routes.put(urlPart, servlet);
			} else {
				servlet = container;
			}
		}
		return servlet;
	}
}
