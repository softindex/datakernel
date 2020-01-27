package io.global.mustache;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheResolver;
import com.github.mustachejava.resolver.ClasspathResolver;
import io.datakernel.di.module.Module;
import org.jetbrains.annotations.Nullable;

import java.io.Reader;
import java.util.concurrent.Executor;

public final class MustacheModule {
	private static final String TEMPLATE_CLASSPATH_FOLDER = "templates";

	private MustacheModule() {
	}

	public static Module create(@Nullable String prefix) {
		return Module.create()
				.bind(MustacheTemplater.class)
				.to(executor ->
						new MustacheTemplater(executor, new DefaultMustacheFactory(PrefixedResolver.create(TEMPLATE_CLASSPATH_FOLDER, prefix))::compile), Executor.class);
	}

	public static Module create() {
		return create(null);
	}

	public static Module createDebug(@Nullable String prefix) {
		return Module.create()
				.bind(MustacheTemplater.class)
				.to(executor ->
						new MustacheTemplater(executor, filename ->
								new DefaultMustacheFactory(PrefixedResolver.create(TEMPLATE_CLASSPATH_FOLDER, prefix)).compile(filename)), Executor.class);
	}

	public static Module createDebug() {
		return createDebug(null);
	}

	private static class PrefixedResolver implements MustacheResolver {
		private final MustacheResolver peer;
		private final String prefix;

		private PrefixedResolver(String classpathFolder, String prefix) {
			this.peer = new ClasspathResolver(classpathFolder);
			this.prefix = prefix;
		}

		public static MustacheResolver create(String classpathFolder, String prefix) {
			return prefix != null ?
					new PrefixedResolver(classpathFolder, prefix) :
					new ClasspathResolver(classpathFolder);
		}

		@Override
		public Reader getReader(String resourceName) {
			Reader reader = peer.getReader(resourceName);
			return reader != null ? reader : peer.getReader(prefix + resourceName);
		}
	}
}
