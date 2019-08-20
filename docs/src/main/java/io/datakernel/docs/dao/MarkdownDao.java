package io.datakernel.docs.dao;

import io.datakernel.docs.model.MarkdownContent;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Set;

public interface MarkdownDao {
    @NotNull MarkdownContent loadContent(@NotNull String relativePath) throws IOException;
    boolean exist(@NotNull String relativePath);
    @NotNull Set<String> indexes();
}
