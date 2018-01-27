package io.datakernel.loader;

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.http.HttpException;

import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

import static java.nio.file.StandardOpenOption.READ;

class SimpleStaticLoaderAsync implements StaticLoader {
	private static final OpenOption[] READ_OPTIONS = {READ};

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final Path root;

	public SimpleStaticLoaderAsync(Eventloop eventloop, ExecutorService executorService, Path root) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.root = root;
	}

	@Override
	public CompletionStage<ByteBuf> getResource(String name) {
		Path file = root.resolve(name).normalize();

		if (!file.startsWith(root)) {
			return Stages.ofException(HttpException.notFound404());
		}

		SettableStage<ByteBuf> stage = SettableStage.create();

		AsyncFile.openAsync(executorService, file, READ_OPTIONS)
				.thenCompose(AsyncFile::readFully)
				.whenComplete((result, throwable) ->
						stage.set(result, throwable instanceof NoSuchFileException ?
								HttpException.notFound404() :
								throwable));
		return stage;
	}
}