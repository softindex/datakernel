package io.datakernel.docs.render;

import io.datakernel.bytebuf.ByteBuf;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class CachedContentRenderer implements ContentRenderer {
	private final ContentRenderer contentRenderer;
	private final Map<String, ByteBuf> cacheMap = new HashMap<>();
	private final Object lock = new Object();

	private CachedContentRenderer(ContentRenderer contentRenderer) {
		this.contentRenderer = contentRenderer;
	}

	public static CachedContentRenderer create(ContentRenderer contentRenderer) {
		return new CachedContentRenderer(contentRenderer);
	}

	@Override
	public ByteBuf render(@NotNull Path templatePath, @NotNull String relativePath) throws IOException, ContentRenderException {
		return doRender(templatePath, relativePath).slice();
	}

	private ByteBuf doRender(@NotNull Path templatePath, @NotNull String relativePath) throws IOException, ContentRenderException {
		synchronized (lock) {
			if (cacheMap.containsKey(relativePath)) {
				return cacheMap.get(relativePath);
			}
		}
		ByteBuf renderedContent = contentRenderer.render(templatePath, relativePath);
		synchronized (lock) {
			if (!cacheMap.containsKey(relativePath)) {
				cacheMap.put(relativePath, renderedContent);
				return renderedContent;
			}
		}
		renderedContent.recycle();
		return cacheMap.get(relativePath);
	}
}
