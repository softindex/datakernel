package io.datakernel.loader;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;

import static io.datakernel.util.Preconditions.checkNotNull;

public class ResourcesNameLoadingService implements EventloopService {
	private final Eventloop eventloop;
	private final ClassLoader loader;
	private final Executor executor;

	private Set<String> names;
	private String resourcePath;

	private ResourcesNameLoadingService(Eventloop eventloop, Executor executor, ClassLoader loader,
			String resourcePath) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.loader = loader;
		this.resourcePath = resourcePath;
	}

	public static ResourcesNameLoadingService create(Eventloop eventloop, Executor executor,
			ClassLoader classLoader, String resourcePath) {
		return new ResourcesNameLoadingService(eventloop, executor, classLoader, resourcePath);
	}

	public static ResourcesNameLoadingService createRoot(Eventloop eventloop, Executor executor,
			ClassLoader classLoader) {
		return new ResourcesNameLoadingService(eventloop, executor, classLoader, "");
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return Promise.ofBlockingCallable(executor,
				() -> {
					Set<String> fileNames = new HashSet<>();
					InputStream in = checkNotNull(loader.getResourceAsStream(resourcePath));
					try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
						String line;
						while ((line = reader.readLine()) != null) {
							fileNames.add(Paths.get(resourcePath).resolve(Paths.get(line)).toString());
						}
						return fileNames;
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				})
				.whenResult(strings -> names = strings)
				.toVoid();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	public Set<String> getNames() {
		return names;
	}

	public boolean contains(String value) {
		return names.contains(value);
	}
}
