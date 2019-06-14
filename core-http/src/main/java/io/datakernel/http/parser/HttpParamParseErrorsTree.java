package io.datakernel.http.parser;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.BiFunction;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;

public class HttpParamParseErrorsTree {
	private static final String DEFAULT_SEPARATOR = ".";

	public static final class Error {
		private final String message;
		private final Object[] args;

		private Error(String message, Object[] args) {
			this.message = message;
			this.args = args;
		}

		public static Error of(String message, Object... args) {
			return new Error(message, args);
		}

		public String getMessage() {
			return message;
		}

		public Object[] getArgs() {
			return args;
		}

		@Override
		public String toString() {
			return String.format(message, args);
		}
	}

	@Nullable
	private List<Error> errors;
	@Nullable
	private Map<String, HttpParamParseErrorsTree> children;

	private HttpParamParseErrorsTree() {}

	public static HttpParamParseErrorsTree create() {
		return new HttpParamParseErrorsTree();
	}

	public static HttpParamParseErrorsTree of(String message, Object... args) {
		return create().with(Error.of(message, args));
	}

	public static HttpParamParseErrorsTree of(@NotNull Error error) {
		return create().with(error);
	}

	public static HttpParamParseErrorsTree of(@NotNull List<Error> errors) {
		return create().with(errors);
	}

	public HttpParamParseErrorsTree with(@NotNull Error error) {
		if (this.errors == null) this.errors = new ArrayList<>();
		this.errors.add(error);
		return this;
	}

	public HttpParamParseErrorsTree with(@NotNull List<Error> errors) {
		if (this.errors == null) this.errors = new ArrayList<>();
		this.errors.addAll(errors);
		return this;
	}

	public HttpParamParseErrorsTree with(@NotNull String id, @NotNull HttpParamParseErrorsTree nestedError) {
		if (children == null) children = new HashMap<>();
		children.merge(id, nestedError, HttpParamParseErrorsTree::merge);
		return this;
	}

	public HttpParamParseErrorsTree merge(HttpParamParseErrorsTree another) {
		if (another.errors != null) {
			if (this.errors == null) {
				this.errors = new ArrayList<>(another.errors);
			} else {
				this.errors.addAll(another.errors);
			}
		}
		if (another.children != null) {
			if (this.children == null) {
				this.children = new HashMap<>(another.children);
			} else {
				for (String key : another.children.keySet()) {
					this.children.merge(key, another.children.get(key), HttpParamParseErrorsTree::merge);
				}
			}
		}
		return this;
	}

	public HttpParamParseErrorsTree with(@NotNull String id, @NotNull Error nestedError) {
		if (children == null) children = new HashMap<>();
		children.computeIfAbsent(id, $ -> new HttpParamParseErrorsTree()).with(nestedError);
		return this;
	}

	public boolean hasErrors() {
		return errors != null || children != null;
	}

	@NotNull
	public List<Error> getErrors() {
		return errors != null ? errors : emptyList();
	}

	@NotNull
	public Set<String> getChildren() {
		return children != null ? children.keySet() : emptySet();
	}

	public HttpParamParseErrorsTree getChild(String id) {
		return children != null ? children.get(id) : null;
	}

	public Map<String, String> toMap() {
		return toMap(String::format);
	}

	public Map<String, String> toMap(String separator) {
		return toMap(String::format, separator);
	}

	public Map<String, List<String>> toMultimap() {
		return toMultimap(String::format);
	}

	public Map<String, List<String>> toMultimap(String separator) {
		return toMultimap(String::format, separator);
	}

	public Map<String, String> toMap(BiFunction<String, Object[], String> formatter) {
		return toMap(formatter, DEFAULT_SEPARATOR);
	}

	public Map<String, String> toMap(BiFunction<String, Object[], String> formatter, String separator) {
		Map<String, String> map = new HashMap<>();
		toMapImpl(this, map, "", formatter, separator);
		return map;
	}

	public Map<String, List<String>> toMultimap(BiFunction<String, Object[], String> formatter, String separator) {
		Map<String, List<String>> multimap = new HashMap<>();
		toMultimapImpl(this, multimap, "", formatter, separator);
		return multimap;
	}

	public Map<String, List<String>> toMultimap(BiFunction<String, Object[], String> formatter) {
		return toMultimap(formatter, DEFAULT_SEPARATOR);
	}

	private static void toMultimapImpl(HttpParamParseErrorsTree errors,
									   Map<String, List<String>> multimap, String prefix,
									   BiFunction<String, Object[], String> formatter,
									   String separator) {
		if (errors.errors != null) {
			multimap.put(prefix, errors.errors.stream().map(error -> formatter.apply(error.message, error.getArgs())).collect(toList()));
		}
		if (errors.children != null) {
			errors.children.forEach((id, child) -> toMultimapImpl(child, multimap, (prefix.isEmpty() ? "" : prefix + separator) + id, formatter, separator));
		}
	}

	private static void toMapImpl(HttpParamParseErrorsTree errors,
								  Map<String, String> map, String prefix,
								  BiFunction<String, Object[], String> formatter,
								  String separator) {
		if (errors.errors != null) {
			Error error = errors.errors.get(0);
			map.put(prefix, formatter.apply(error.message, error.getArgs()));
		}
		if (errors.children != null) {
			errors.children.forEach((id, child) -> toMapImpl(child, map, (prefix.isEmpty() ? "" : prefix + separator) + id, formatter, separator));
		}
	}
}
