package io.datakernel.remotefs;

import io.datakernel.exception.UncheckedException;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.datakernel.remotefs.FsClient.*;
import static java.nio.charset.StandardCharsets.US_ASCII;

public final class RemoteFsUtils {
	private static final Pattern ANY_GLOB_METACHARS = Pattern.compile("[*?{}\\[\\]\\\\]");
	private static final Pattern UNESCAPED_GLOB_METACHARS = Pattern.compile("(?<!\\\\)(?:\\\\\\\\)*[*?{}\\[\\]]");

	public static final Throwable[] KNOWN_ERRORS = {
			FILE_NOT_FOUND,
			FILE_EXISTS,
			BAD_PATH,
			OFFSET_TOO_BIG,
			LENGTH_TOO_BIG,
			BAD_RANGE,
			MOVING_DIRS,
			UNSUPPORTED_REVISION
	};

	private RemoteFsUtils() {
		throw new AssertionError("nope.");
	}

	/**
	 * Escapes any glob metacharacters so that given path string can ever only match one file.
	 *
	 * @param path path that potentially can contain glob metachars
	 * @return escaped glob which matches only a file with that name
	 */
	public static String escapeGlob(String path) {
		return ANY_GLOB_METACHARS.matcher(path).replaceAll("\\\\$0");
	}

	/**
	 * Checks if given glob can match more than one file.
	 *
	 * @param glob the glob to check.
	 * @return <code>true</code> if given glob can match more than one file.
	 */
	public static boolean isWildcard(String glob) {
		return UNESCAPED_GLOB_METACHARS.matcher(glob).find();
	}

	/**
	 * Returns a {@link PathMatcher} for given glob
	 *
	 * @param glob a glob string
	 * @return a path matcher for the glob string
	 */
	public static PathMatcher getGlobPathMatcher(String glob) {
		return FileSystems.getDefault().getPathMatcher("glob:" + glob);
	}

	/**
	 * Same as {@link #getGlobPathMatcher(String)} but returns a string predicate.
	 *
	 * @param glob a glob string
	 * @return a predicate for the glob string
	 */
	public static Predicate<String> getGlobStringPredicate(String glob) {
		PathMatcher matcher = getGlobPathMatcher(glob);
		return str -> matcher.matches(Paths.get(str));
	}

	private static UserDefinedFileAttributeView attrs(Path path) {
		return Files.getFileAttributeView(path, UserDefinedFileAttributeView.class);
	}

	@Nullable
	public static Long getLongAttribute(Path path, String name) throws IOException {
		try {
			UserDefinedFileAttributeView attrs = attrs(path);
			ByteBuffer buffer = ByteBuffer.allocate(20);
			attrs.read(name, buffer);
			buffer.flip();
			return Long.valueOf(new String(buffer.array(), buffer.position(), buffer.limit(), US_ASCII));
		} catch (FileSystemException | NumberFormatException ignored) {
			// generic FileSystemException (instead of, say, NoSuchFileException)
			// is thrown when user_xattrs is unsupported or there is no attribute
			return null;
		}
	}

	public static void setLongAttribute(Path path, String name, long value) throws IOException {
		attrs(path).write(name, ByteBuffer.wrap(String.valueOf(value).getBytes(US_ASCII)));
	}

	@Nullable
	public static Boolean getBooleanAttribute(Path path, String name) throws IOException {
		try {
			UserDefinedFileAttributeView attrs = attrs(path);
			ByteBuffer buffer = ByteBuffer.allocate(attrs.size(name));
			attrs.read(name, buffer);
			buffer.flip();

			return Boolean.parseBoolean(new String(buffer.array(), buffer.position(), buffer.limit(), US_ASCII));
		} catch (FileSystemException ignored) {
			return null;
		}
	}

	public static void setBooleanAttribute(Path path, String name, boolean value) throws IOException {
		attrs(path).write(name, ByteBuffer.wrap("true".getBytes(US_ASCII)));
	}

	public static void removeAttribute(Path path, String name) throws IOException {
		try {
			UserDefinedFileAttributeView attrs = attrs(path);
			attrs.delete(name);
		} catch (FileSystemException ignored) {
		}
	}

	public static int getErrorCode(Throwable e) {
		if (e == FILE_NOT_FOUND) return 1;
		if (e == FILE_EXISTS) return 2;
		if (e == BAD_PATH) return 3;
		if (e == OFFSET_TOO_BIG) return 4;
		if (e == LENGTH_TOO_BIG) return 5;
		if (e == BAD_RANGE) return 6;
		if (e == MOVING_DIRS) return 7;
		if (e == UNSUPPORTED_REVISION) return 8;
		return 0;
	}

	static void checkRange(long size, long offset, long length) {
		if (offset < -1 || length < -1) {
			throw new UncheckedException(BAD_RANGE);
		}
		if (offset > size) {
			throw new UncheckedException(OFFSET_TOO_BIG);
		}
		if (length != -1 && offset + length > size) {
			throw new UncheckedException(LENGTH_TOO_BIG);
		}
	}
}
