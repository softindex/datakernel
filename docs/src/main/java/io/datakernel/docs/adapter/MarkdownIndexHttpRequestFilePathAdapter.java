package io.datakernel.docs.adapter;

import java.nio.file.Files;
import java.nio.file.Path;

public class MarkdownIndexHttpRequestFilePathAdapter implements MarkdownIndexAdapter<String> {
	private static final String EXTENSION = "md";
	private static final String INDEX = "index";
	private static final String EMPTY = "";
	private static final String DOT = ".";
	private final Path sourceFilesPath;

	private MarkdownIndexHttpRequestFilePathAdapter(Path sourceFilesPath) {
		this.sourceFilesPath = sourceFilesPath;
	}

	public static MarkdownIndexHttpRequestFilePathAdapter create(Path sourceFilesPath) {
		return new MarkdownIndexHttpRequestFilePathAdapter(sourceFilesPath);
	}

	@Override
	public String resolve(String url) {
		url = url.replaceFirst("/?(docs/)?", EMPTY)
				.replaceFirst("\\.\\w+", DOT + EXTENSION);
		Path resolvedPath = sourceFilesPath.resolve(url);
		return (Files.isDirectory(resolvedPath) ?
				resolvedPath.resolve(INDEX + DOT + EXTENSION) :
				resolvedPath).toString();
	}
}
