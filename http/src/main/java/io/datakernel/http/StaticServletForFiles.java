package io.datakernel.http;

import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.file.File;

import java.net.URL;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

import static java.nio.file.StandardOpenOption.READ;

public final class StaticServletForFiles extends StaticServlet {
	private final NioEventloop eventloop;
	private final ExecutorService executor;
	private final Path storage;

	private StaticServletForFiles(NioEventloop eventloop, ExecutorService executor, Path storage) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.storage = storage;
	}

	public static StaticServletForFiles create(NioEventloop eventloop, ExecutorService executor, URL url) {
		Path path = Paths.get(url.getPath());
		return new StaticServletForFiles(eventloop, executor, path);
	}

	@Override
	protected final void doServeAsync(String name, final ForwardingResultCallback<ByteBuf> callback) {
		AsyncFile.open(eventloop, executor, storage.resolve(name),
				new OpenOption[]{READ}, new ForwardingResultCallback<File>(callback) {
					@Override
					public void onResult(File file) {
						file.readFully(callback);
					}
				});
	}
}
