package io.datakernel.loader;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.http.HttpException;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.ExecutorService;

class StaticLoaderClassPath implements StaticLoader {
	private final ExecutorService executorService;
	private final ClassLoader classLoader;

	public StaticLoaderClassPath(ExecutorService executorService, @Nullable Class<?> classLoader) {
		this.executorService = executorService;
		this.classLoader = classLoader == null ?
				Thread.currentThread().getContextClassLoader() :
				classLoader.getClassLoader();
	}

	@Override
	public Promise<ByteBuf> getResource(String name) {
		URL file = classLoader.getResource(name);

		if (file == null) {
			return Promise.ofException(HttpException.notFound404());
		}

	    return Promise.ofCallable(executorService, () -> ByteBuf.wrapForReading(loadResource(file)))
                .thenComposeEx((buf, e) ->
		                Promise.of(buf, e instanceof NoSuchFileException ? HttpException.notFound404() : e));
    }

	private byte[] loadResource(URL file) throws IOException {
		try (InputStream in = file.openStream()) {
			// reading file as resource
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			byte[] buffer = new byte[4096];
			int size;
			while ((size = in.read(buffer)) != -1) {
				out.write(buffer, 0, size);
			}
			return out.toByteArray();
		}
	}
}
