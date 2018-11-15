package io.datakernel.launchers.http;

import com.google.inject.*;
import com.google.inject.name.Names;
import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.*;
import io.datakernel.jmx.JmxModule;
import io.datakernel.jmx.KeyWithWorkerData;
import io.datakernel.loader.StaticLoader;
import io.datakernel.loader.StaticLoaders;
import io.datakernel.util.MemSize;
import io.datakernel.util.RecursiveType;
import io.datakernel.util.ReflectionUtils;
import io.datakernel.util.StringFormatUtils;
import io.datakernel.util.guice.GuiceUtils;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPools;

import java.io.UnsupportedEncodingException;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static io.datakernel.launchers.Initializers.ofEventloop;
import static io.datakernel.launchers.Initializers.ofHttpServer;
import static io.datakernel.util.Preconditions.checkState;
import static io.datakernel.util.guice.GuiceUtils.prettyPrintSimpleKeyName;
import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class JmxHttpModule extends AbstractModule {
	private Set<Key<?>> singletons = new HashSet<>();
	private Set<KeyWithWorkerData> workers = new HashSet<>();

	private List<InetSocketAddress> serverAddresses = null;
	private Map<Class<?>, Function<Object, String>> toStringConverters = new LinkedHashMap<>();

	private JmxHttpModule() {
	}

	public static JmxHttpModule create() {
		return new JmxHttpModule();
	}

	@SuppressWarnings("unchecked")
	public static JmxHttpModule defaultInstance() {
		JmxHttpModule module = new JmxHttpModule();
		return module
				.withToStringConverter(byte[].class, Arrays::toString)
				.withToStringConverter(boolean[].class, Arrays::toString)
				.withToStringConverter(short[].class, Arrays::toString)
				.withToStringConverter(int[].class, Arrays::toString)
				.withToStringConverter(long[].class, Arrays::toString)
				.withToStringConverter(float[].class, Arrays::toString)
				.withToStringConverter(double[].class, Arrays::toString)
				.withToStringConverter(Duration.class, StringFormatUtils::formatDuration)
				.withToStringConverter(Period.class, StringFormatUtils::formatPeriod)
				.withToStringConverter(Instant.class, StringFormatUtils::formatInstant)
				.withToStringConverter(LocalDateTime.class, StringFormatUtils::formatLocalDateTime)
				.withToStringConverter(MemSize.class, StringFormatUtils::formatMemSize)
				.withToStringConverter(List.class, list -> ((List<Object>) list).stream().map(module::toString).collect(joining("\n")))
				.withToStringConverter(Object[].class, array -> Arrays.stream(array).map(module::toString).collect(joining("\n")));
	}

	public JmxHttpModule withServer(List<InetSocketAddress> listenAddresses) {
		checkState(serverAddresses == null, "Cannot set server addresses more than once");
		serverAddresses = listenAddresses;
		return this;
	}

	public JmxHttpModule withServer(InetSocketAddress listenAddress) {
		return withServer(singletonList(listenAddress));
	}

	public JmxHttpModule withServer(int port) {
		return withServer(new InetSocketAddress(port));
	}

	@SuppressWarnings("unchecked")
	public <T> JmxHttpModule withToStringConverter(Class<T> type, Function<T, String> converter) {
		toStringConverters.put(type, (Function<Object, String>) converter);
		return this;
	}

	@Override
	protected void configure() {
		JmxModule.bindKeyListeners(binder(), this, b -> singletons.add(b.getKey()), b -> {
			Key<?> key = b.getKey();
			WorkerPool workerPool = GuiceUtils.extractWorkerPool(b);
			Integer workerId = GuiceUtils.extractWorkerId(b);
			assert workerPool != null && workerId != null : b;
			workers.add(new KeyWithWorkerData(key, workerPool, workerId));
		});

		if (serverAddresses != null) {
			install(new AbstractModule() {

				@Inject
				@JmxHttp
				AsyncHttpServer jmxHttpServer;

				@Override
				protected void configure() {
					requestInjection(this);
				}

				@Provides
				@Singleton
				@JmxHttp
				Eventloop provideEventloop(Config config) {
					return Eventloop.create()
							.initialize(ofEventloop(config.getChild("jmx.httpServer.eventloop")));
				}

				@Provides
				@Singleton
				@JmxHttp
				AsyncHttpServer provideJmxHttpServer(
						@JmxHttp Eventloop eventloop,
						@JmxHttp AsyncServlet jmxServlet,
						OptionalDependency<Config> maybeConfig
				) {
					AsyncHttpServer server = AsyncHttpServer.create(eventloop, jmxServlet);
					maybeConfig.ifPresent(config ->
							server.initialize(ofHttpServer(config.getChild("jmx.httpServer")
									.with("listenAddresses", serverAddresses.stream()
											.map(s -> s.getHostString() + ":" + s.getPort())
											.collect(joining(","))))));
					return server;
				}
			});
		}
	}

	@Provides
	@Singleton
	@JmxHttp
	ExecutorService provideExecutor() {
		return Executors.newSingleThreadExecutor();
	}

	@Provides
	@Singleton
	@JmxHttp
	AsyncServlet provideJmxServlet(@JmxHttp ExecutorService executor, Injector injector) {
		return new JmxAsyncServlet(StaticLoaders.ofClassPath(executor), injector);
	}

	@BindingAnnotation
	@Target({FIELD, PARAMETER, METHOD})
	@Retention(RUNTIME)
	public @interface JmxHttp {}

	private static final Pattern KEY_PARAM_PATTERN =
			Pattern.compile("^(.*?)(?:<(.*?)>)?(?:@(.*?))?(?:\\[(\\d+)])?$");
	//                    ^---^    ^---^       ^---^       ^------^
	//                      ↑        ↑           ↑            ↑ optional number in brackets - the worker id
	//                      ↑        ↑           ↑ optional part after @ and before optional worker id - the annotation
	//                      ↑        ↑ optional part inside < and > - list of generics
	//                      ↑ part before optional < - raw class name

	private String toString(@Nullable Object object) {
		if (object == null) {
			return "";
		}
		for (Entry<Class<?>, Function<Object, String>> entry : toStringConverters.entrySet()) {
			if (entry.getKey().isAssignableFrom(object.getClass())) {
				return entry.getValue().apply(object);
			}
		}
		return object.toString();
	}

	private static Promise<Class<?>> getClass(String className) {
		try {
			return Promise.of(Class.forName(className));
		} catch (ClassNotFoundException e) {
			return Promise.ofException(HttpException.ofCode(404, "Class " + className + " not found"));
		}
	}

	private static String htmlEscape(String string) {
		return string.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\n", "<br>").replace(" ", "&nbsp;");
	}

	private static String indexHTML(Node<String, String> tree, String link) {
		StringBuilder sb = new StringBuilder();
		indexHTML(tree, link, new int[]{0}, sb);
		return sb.toString();
	}

	private static void indexHTML(Node<String, String> tree, String link, int[] index, StringBuilder sb) {
		tree.children.values().stream()
				.sorted(Comparator.comparing(t -> t.key != null ? t.key : ""))
				.forEach(subtree -> {
					if (subtree.children.isEmpty()) {
						String url = link + "?key=";
						try {
							url += URLEncoder.encode(subtree.value != null ? subtree.value : "", UTF_8.name());
						} catch (UnsupportedEncodingException e) {
							throw new AssertionError(e); // no utf-8?
						}
						sb.append("<li><a href=\"")
								.append(url)
								.append("\">")
								.append(htmlEscape(subtree.key != null ? subtree.key : ""))
								.append("</a></li>");
						return;
					}
					sb.append("<input type=\"checkbox\" id=\"item-")
							.append(index[0])
							.append("\" checked><label for=\"item-")
							.append(index[0])
							.append("\">")
							.append(subtree.key)
							.append("</label><ul>");
					index[0]++;
					indexHTML(subtree, link, index, sb);
					sb.append("</ul><br>");
				});
	}

	private String tableHTML(Object instance) {
		Node<String, Object> tree = new Node<>(null, null, null);
		ReflectionUtils.getJmxAttributes(instance)
				.forEach((k, v) -> {
					Node<String, Object> node = tree;
					String[] parts = k.split("_");
					for (String part : parts) {
						Node<String, Object> finalNode = node;
						node = node.children.computeIfAbsent(part, $ -> new Node<>(finalNode, part, null));
					}
					node.value = v;
				});
		StringBuilder sb = new StringBuilder();
		tableHTML(tree, "", new int[]{0}, "", 0, sb);
		return sb.toString();
	}

	private void tableHTML(Node<String, Object> tree, String indent, int[] folderVar, String foldable, int folder, StringBuilder table) {
		tree.children.values().forEach(subtree -> {
			if (subtree.children.isEmpty()) {
				table
						.append("<tr class=\"hidden\" data-folder=\"")
						.append(folder)
						.append("\" data-foldable=\"")
						.append(foldable)
						.append("\"><td><pre>")
						.append(indent)
						.append("</pre>")
						.append(subtree.key)
						.append("</td><td>")
						.append(htmlEscape(toString(subtree.value)))
						.append("</td></tr>");
				return;
			}
			folderVar[0]++;
			tableHTML(subtree, indent + "  ", folderVar, foldable + " " + folderVar[0], folderVar[0], table
					.append("<tr class=\"hidden\" data-managing=\"")
					.append(folderVar[0])
					.append("\" data-folder=\"")
					.append(folder)
					.append("\" data-foldable=\"")
					.append(foldable)
					.append("\"><td><pre>")
					.append(indent)
					.append("</pre><pre>+</pre>")
					.append(subtree.key)
					.append("</pre></td><td>")
					.append(htmlEscape(toString(subtree.value)))
					.append("</td></tr>"));
		});
	}

	private class JmxAsyncServlet implements AsyncServlet {
		private final StaticLoader loader;
		private final Injector injector;

		private byte[] cachedIndex = null;

		private JmxAsyncServlet(StaticLoader loader, Injector injector) {
			this.loader = loader;
			this.injector = injector;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Promise<HttpResponse> serve(HttpRequest request) throws ParseException {
			String keyParam = request.getQueryParameterOrNull("key");

			if ("/favicon.ico".equals(request.getPath())) { // if somehow it got to this servlet
				return Promise.ofException(HttpException.notFound404());
			}

			if (keyParam == null) {
				if (cachedIndex != null) {
					return Promise.of(HttpResponse.ok200().withBody(cachedIndex));
				}

				Node<String, String> tree = new Node<>(null, null, null);

				Stream.concat(singletons.stream().map(KeyWithWorkerData::new), workers.stream())
						.filter(desc -> ReflectionUtils.isBean(desc.getKey().getTypeLiteral().getRawType()))
						.forEach(desc -> {
							Key<?> key = desc.getKey();
							TypeLiteral<?> literal = key.getTypeLiteral();
							String name = prettyPrintSimpleKeyName(key);
							String pkg = RecursiveType.of(key.getTypeLiteral().getType()).getPackage();
							String[] path = pkg != null ? pkg.split("\\.") : new String[]{"unknown_package"};

							Node<String, String> subtree = tree;
							for (String part : path) {
								Node<String, String> finalSubtree = subtree;
								subtree = subtree.children.computeIfAbsent(part, $ -> new Node<>(finalSubtree, part, null));
							}
							String keyStr = literal.toString() + (key.getAnnotation() != null ? key.getAnnotation().toString() : "");
							if (desc.getWorkerId() == -1) {
								subtree.children.put(name, new Node<>(subtree, name, keyStr));
								return;
							}
							name += " workers:";
							Node<String, String> workers = new Node<>(subtree, name, null);
							for (int i = 0; i < desc.getWorkerId(); i++) {
								String k = "id: " + i;
								workers.children.put(k, new Node<>(workers, k, keyStr + '[' + i + ']'));
							}
							subtree.children.put(name, workers);
						});

				return loader.getResource("index_template.html")
						.thenApply(buf -> HttpResponse.ok200()
								.withBody(cachedIndex = String.format(buf.asString(UTF_8), indexHTML(tree, request.getPath())).getBytes(UTF_8)));
			}

			Matcher matcher = KEY_PARAM_PATTERN.matcher(keyParam);
			if (!matcher.find()) {
				return Promise.ofException(HttpException.ofCode(400, "Wrong key format"));
			}
			String clsName = matcher.group(1);
			String generics = matcher.group(2);
			String annotation = matcher.group(3);
			String workerId = matcher.group(4);

			return Promises.toTuple(
					JmxHttpModule.getClass(clsName),
					Promises.toList(
							Arrays.stream(generics != null ? generics.split(", ") : new String[0])
									.map(JmxHttpModule::getClass)))
					.thenCompose(tuple -> {
						Type type = RecursiveType.of(
								tuple.getValue1(),
								tuple.getValue2()
										.stream()
										.map(RecursiveType::of)
										.collect(toList()))
								.getType();
						if (annotation == null) {
							return Promise.of(Key.get(type));
						}
						if (annotation.startsWith("com.google.inject.name.Named(value=")) {
							return Promise.of(Key.get(type, Names.named(annotation.substring(35, annotation.length() - 1))));
						}
						if (!annotation.endsWith("()")) {
							return Promise.ofException(HttpException.ofCode(501, "Instance annotations except @Named are not supported"));
						}
						return JmxHttpModule.getClass(annotation.substring(0, annotation.length() - 2))
								.thenApply(annCls -> Key.get(type, (Class<Annotation>) annCls));
					})
					.thenCompose(key -> {
						Object instance = null;
						if (workerId != null) {
							Binding<WorkerPools> wpBinding = injector.getExistingBinding(Key.get(WorkerPools.class));
							if (wpBinding == null) {
								return Promise.ofException(HttpException.ofCode(404, "No worker pool exists"));
							}
							List<?> instances = wpBinding.getProvider().get().getAllObjects(key); // TODO: this will fail with multiple worker pools, fix it
							int id = Integer.parseInt(workerId);
							if (id >= instances.size()) {
								return Promise.ofException(HttpException.ofCode(404, "Worker " + key + " with id " + id + " not found"));
							}
							instance = instances.get(id);
						} else {
							Binding<?> binding = injector.getExistingBinding(key);
							if (binding != null) {
								instance = binding.getProvider().get();
							}
						}
						if (instance == null) {
							return Promise.ofException(HttpException.ofCode(404, "Key " + key + " not found."));
						}
						Object finalInstance = instance;
						return loader.getResource("table_template.html")
								.thenApply(buf -> HttpResponse.ok200()
										.withBody(String.format(
												buf.asString(UTF_8),
												htmlEscape(prettyPrintSimpleKeyName(key)) + (workerId != null ? " (worker id: " + workerId + ")" : ""),
												tableHTML(finalInstance)
										).getBytes(UTF_8)));
					});
		}
	}

	private static class Node<K, V> {
		@Nullable
		final Node parent;

		@Nullable
		final K key;

		@Nullable
		V value;

		final Map<K, Node<K, V>> children = new LinkedHashMap<>();

		public Node(@Nullable Node parent, @Nullable K key, @Nullable V value) {
			this.parent = parent;
			this.key = key;
			this.value = value;
		}
	}
}
