package io.global.mustache;

import com.github.mustachejava.Mustache;

@FunctionalInterface
public interface MustacheSupplier {
	Mustache getMustache(String filename);
}
