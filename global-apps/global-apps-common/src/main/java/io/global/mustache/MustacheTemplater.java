package io.global.mustache;

import com.github.mustachejava.util.InternalArrayList;
import io.datakernel.bytebuf.util.ByteBufWriter;
import io.datakernel.common.ref.Ref;
import io.datakernel.http.HttpResponse;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static io.datakernel.http.ContentTypes.HTML_UTF_8;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
import static io.datakernel.http.HttpHeaders.CACHE_CONTROL;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static java.util.Collections.emptyMap;

public final class MustacheTemplater {
	private final Executor executor;
	private final MustacheSupplier mustacheSupplier;
	private final Map<String, Object> staticContext = new HashMap<>();

	public MustacheTemplater(Executor executor, MustacheSupplier mustacheSupplier) {
		this.executor = executor;
		this.mustacheSupplier = mustacheSupplier;
	}

	public void put(String name, @Nullable Object object) {
		staticContext.put(name, object);
	}

	public void clear() {
		staticContext.clear();
	}

	@SuppressWarnings("SuspiciousMethodCalls")
	public Promise<HttpResponse> render(int code, String templateName, Map<String, @Nullable Object> scope, boolean useGzip) {
		Map<String, Object> map = new HashMap<>(scope);
		map.putAll(staticContext);
		List<Promise<?>> promisesToWait = new ArrayList<>();

		for (Map.Entry<String, Object> entry : map.entrySet()) {
			Object value = entry.getValue();
			if (value instanceof Ref) {
				entry.setValue(map.get(((Ref<?>) value).get()));
			}
			if (!(value instanceof Promise)) {
				continue;
			}
			Promise<?> promise = (Promise<?>) value;
			if (promise.isResult()) {
				entry.setValue(promise.getResult());
			} else {
				promisesToWait.add(promise.whenResult(entry::setValue));
			}
		}
		return Promises.all(promisesToWait)
				.then($ ->
						Promise.ofBlockingCallable(executor, () -> {
							InternalArrayList<Object> context = new InternalArrayList<>();
							Object spread;
							if ((spread = map.remove(".")) != null) {
								context.add(spread);
							}
							context.add(map);
							ByteBufWriter writer = new ByteBufWriter();
							mustacheSupplier.getMustache(templateName + ".mustache").execute(writer, context);

							HttpResponse httpResponse = HttpResponse.ofCode(code)
									.withBody(writer.getBuf())
									.withHeader(CACHE_CONTROL, "no-store")
									.withHeader(CONTENT_TYPE, ofContentType(HTML_UTF_8));

							if (useGzip) {
								httpResponse.setBodyGzipCompression();
							}
							return httpResponse;
						}));
	}

	public Promise<HttpResponse> render(String templateName, Map<String, @Nullable Object> scope, boolean useGzip) {
		return render(200, templateName, scope, useGzip);
	}

	public Promise<HttpResponse> render(String templateName, boolean useGzip) {
		return render(200, templateName, emptyMap(), useGzip);
	}
}
