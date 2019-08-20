package io.datakernel.docs.dao;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllBytes;

/**
 * Is used to set the root dir, and receive file from its
 */
public class FileResourceDao implements ResourceDao {
	private static final String ROOT = "/";
	private static final String EMPTY = "";
	private final Path rootPath;

	private FileResourceDao(Path rootPath) {
		this.rootPath = rootPath;
	}

	public static FileResourceDao create(Path path) throws FileNotFoundException {
		if (!Files.isDirectory(path) || !Files.exists(path)) {
			throw new FileNotFoundException(path.toString());
		}
		return new FileResourceDao(path);
	}

	@Override
	public String getResource(String resourceName) throws IOException {
		return new String(readAllBytes(rootPath.resolve(
				resourceName.startsWith(ROOT) ?
						resourceName.replaceFirst(ROOT, EMPTY) :
						resourceName)),
				UTF_8);
	}

	@Override
	public boolean exist(String resourceName) {
		return Files.exists(
				rootPath.resolve(resourceName.startsWith(ROOT) ?
						resourceName.replaceFirst(ROOT, EMPTY) :
						resourceName));
	}
}
