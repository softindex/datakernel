package io.datakernel.loader;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

public class StaticLoaders {

	private StaticLoaders() {
	}

	public static StaticLoader ofClassPath(ExecutorService executorService) {
		return new StaticLoaderClassPath(executorService, null);
	}

	public static StaticLoader ofClassPath(ExecutorService executorService, Class<?> relativeClass) {
		return new StaticLoaderClassPath(executorService, relativeClass);
	}

	public static StaticLoader ofPath(ExecutorService executorService, Path dir) {
		return new SimpleStaticLoaderAsync(executorService, dir);
	}

	public static StaticLoader ofFile(ExecutorService executorService, File dir) {
		return new SimpleStaticLoaderAsync(executorService, dir.toPath());
	}
}