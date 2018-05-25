package io.datakernel.loader;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.loader.cache.Cache;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

public class StaticLoaders {

	private StaticLoaders() {
	}

	public static StaticLoader ofClassPath(Eventloop eventloop, ExecutorService executorService) {
		return new StaticLoaderClassPath(eventloop, executorService, null);
	}

	public static StaticLoader ofClassPath(Eventloop eventloop, ExecutorService executorService, Class<?> relativeClass) {
		return new StaticLoaderClassPath(eventloop, executorService, relativeClass);
	}

	public static StaticLoader ofPath(Eventloop eventloop, ExecutorService executorService, Path dir) {
		return new SimpleStaticLoaderAsync(eventloop, executorService, dir);
	}

	public static StaticLoader ofFile(Eventloop eventloop, ExecutorService executorService, File dir) {
		return new SimpleStaticLoaderAsync(eventloop, executorService, dir.toPath());
	}

	public static StaticLoader ofCache(StaticLoader loader, Cache cache) {
		return new CachedStaticLoader(loader, cache);
	}

	public static StaticLoader ofPredicate(StaticLoader loader, Predicate<String> predicate) {
		return new PredicateNamesStaticLoader(loader, predicate);
	}
}