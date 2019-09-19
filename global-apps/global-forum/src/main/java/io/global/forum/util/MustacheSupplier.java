package io.global.forum.util;

import com.github.mustachejava.Mustache;

@FunctionalInterface
public interface MustacheSupplier {

	Mustache getMustache(String filename);
}
