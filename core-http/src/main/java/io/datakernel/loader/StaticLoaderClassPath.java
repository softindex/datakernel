package io.datakernel.loader;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;

import static java.lang.ClassLoader.getSystemClassLoader;

class StaticLoaderClassPath implements StaticLoader {
	private static final String ROOT = "/";
	private static final int ROOT_OFFSET = 1;
	@Nullable
	private final Executor executor;
	private final ClassLoader classLoader;
	private final String root;

	private StaticLoaderClassPath(@Nullable Executor executor, @NotNull ClassLoader classLoader, @NotNull String root) {
		this.root = root;
		this.executor = executor;
		this.classLoader = classLoader;
	}

	public static StaticLoaderClassPath create(String root) {
		return create(null, getSystemClassLoader(), root);
	}

	public static StaticLoaderClassPath create(@Nullable Executor executor, @NotNull ClassLoader classLoader, @NotNull String root) {
		if (root.startsWith(ROOT)) {
			root = root.substring(ROOT_OFFSET);
		}
		if (!root.endsWith(ROOT) && root.length() > 0) {
			root = root + ROOT;
		}

		return new StaticLoaderClassPath(executor, classLoader, root);
	}

	@Override
	public Promise<ByteBuf> load(String name) {
		String path = root;
		int begin = 0;
		if (name.startsWith(ROOT)) {
			begin++;
		}
		path += name.substring(begin);

		String finalPath = path;

		return Promise.ofBlockingCallable(executor, () -> {
			URL resource = classLoader.getResource(finalPath);
			if (resource == null) {
				throw NOT_FOUND_EXCEPTION;
			}

			URLConnection connection = resource.openConnection();

			if (connection instanceof JarURLConnection) {
				if (((JarURLConnection) connection).getJarEntry().isDirectory()) {
					throw IS_A_DIRECTORY;
				}
			} else if ("file".equals(resource.getProtocol())) {
				Path filePath = Paths.get(resource.getPath());
				if (!Files.isRegularFile(filePath)) {
					if (Files.isDirectory(filePath)) {
						throw IS_A_DIRECTORY;
					} else {
						throw NOT_FOUND_EXCEPTION;
					}
				}
			}
			return ByteBuf.wrapForReading(loadResource(connection));
		});
	}

	private byte[] loadResource(URLConnection connection) throws IOException {
		// reading file as resource
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		byte[] buffer = new byte[8192];
		int size;
		try (InputStream stream = connection.getInputStream()) {
			while ((size = stream.read(buffer)) != -1) {
				out.write(buffer, 0, size);
			}
		}
		return out.toByteArray();
	}
}
