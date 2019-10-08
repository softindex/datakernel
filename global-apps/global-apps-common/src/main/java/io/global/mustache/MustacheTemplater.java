package io.global.mustache;

import com.github.mustachejava.util.InternalArrayList;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.http.HttpResponse;
import io.datakernel.util.ref.Ref;
import io.datakernel.writer.ByteBufWriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static io.datakernel.http.ContentTypes.HTML_UTF_8;
import static io.datakernel.http.HttpHeaderValue.ofContentType;
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

	public void put(String name, Object object) {
		staticContext.put(name, object);
	}

	public void clear() {
		staticContext.clear();
	}

	@SuppressWarnings("SuspiciousMethodCalls")
	public Promise<HttpResponse> render(int code, String templateName, Map<String, Object> scope) {
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
							return HttpResponse.ofCode(code)
									.withBody(writer.getBuf())
									.withHeader(CONTENT_TYPE, ofContentType(HTML_UTF_8));
						}));
	}

	public Promise<HttpResponse> render(String templateName, Map<String, Object> scope) {
		return render(200, templateName, scope);
	}

	public Promise<HttpResponse> render(String templateName) {
		return render(200, templateName, emptyMap());
	}
}
