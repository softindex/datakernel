package io.datakernel.loader;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.http.HttpException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.ExecutorService;

import static io.datakernel.bytebuf.ByteBuf.wrapForReading;

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
	public Stage<ByteBuf> getResource(String name) {
		URL file = classLoader.getResource(name);

		if (file == null) {
			return Stage.ofException(HttpException.notFound404());
		}

        SettableStage<ByteBuf> stage = new SettableStage<>();
	    Stage.ofCallable(executorService, () -> wrapForReading(loadResource(file)))
                .whenComplete((result, throwable) ->
                        stage.set(result, throwable instanceof NoSuchFileException ?
                                HttpException.notFound404() :
                                throwable));
        return stage;
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
