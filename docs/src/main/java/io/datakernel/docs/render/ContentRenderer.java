package io.datakernel.docs.render;

import io.datakernel.bytebuf.ByteBuf;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Path;

public interface ContentRenderer {
	ByteBuf render(@NotNull Path templatePath, @NotNull String relativePath) throws IOException, ContentRenderException;
}
