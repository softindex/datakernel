package io.datakernel.docs.dao;

import io.datakernel.docs.model.MarkdownContent;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.util.Collections.emptySet;
import static java.util.regex.Pattern.DOTALL;
import static java.util.regex.Pattern.MULTILINE;
import static org.apache.pdfbox.util.Charsets.UTF_8;

@SuppressWarnings("SameParameterValue")
public final class FileMarkdownDao implements MarkdownDao {
	private static final Logger logger = LoggerFactory.getLogger(FileMarkdownDao.class);
	private static final String PROPERTIES_PATTERN = "([\\w-]+):\\s*(.*)";
	private static final String PROPERTIES_BLOCK_PATTERN = "---(.*)---";

	private final Pattern propertiesBlockPattern = Pattern.compile(PROPERTIES_BLOCK_PATTERN, DOTALL);
	private final Pattern propertiesPattern = Pattern.compile(PROPERTIES_PATTERN, MULTILINE);
	private final Path resourcePath;
	private Set<String> indexes;

	private FileMarkdownDao(Path resourcePath) {
		this.resourcePath = resourcePath;
	}

	public static FileMarkdownDao create(Path resourcePath) {
		return new FileMarkdownDao(resourcePath);
	}

	@NotNull
	@Override
	public MarkdownContent loadContent(@NotNull String relativePath) throws IOException {
		byte[] readContent = Files.readAllBytes(Paths.get(relativePath));
		String content = new String(readContent, UTF_8);
		return MarkdownContent.of(parseProperties(content), content);
	}

	@Override
	public boolean exist(@NotNull String relativePath) {
		return indexes().contains(relativePath);
	}

	@NotNull
	@Override
	public Set<String> indexes() {
		return indexes == null ? indexes = findIndexes() : indexes;
	}

	private Set<String> findIndexes() {
		Set<String> indexes = new HashSet<>();
		try {
			Files.walkFileTree(resourcePath, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
					indexes.add(file.toString());
					return CONTINUE;
				}
			});
		} catch (IOException e) {
			logger.warn("Path is wrong", e);
			return emptySet();
		}
		return indexes;
	}

	private Map<String, String> parseProperties(String content) {
		Map<String, String> properties = new HashMap<>();
		Matcher blockMatch = propertiesBlockPattern.matcher(content);
		if (blockMatch.find()) {
			Matcher propsMatch = propertiesPattern.matcher(blockMatch.group(1));
			while (propsMatch.find()) {
				properties.put(propsMatch.group(1), propsMatch.group(2));
			}
		}
		return properties;
	}
}
