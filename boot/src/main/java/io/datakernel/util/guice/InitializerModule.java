package io.datakernel.util.guice;

import com.google.inject.*;
import com.google.inject.matcher.AbstractMatcher;
import com.google.inject.spi.ProvisionListener;
import io.datakernel.util.Initializable;
import io.datakernel.util.Initializer;
import io.datakernel.util.RecursiveType;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <h3>This module adds some Guice <i>magic</i> to your applications.</h3>
 * <p>
 * It listens for any elements that extend {@link Initializable} interface.
 * For each of those, it searches for the appropriate {@link Initializer} element.
 * If initializer is found, it applies it to the initializable element.
 * </p>
 */
public final class InitializerModule extends AbstractModule {
	private InitializerModule() {
	}

	public static InitializerModule create() {
		return new InitializerModule();
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected void configure() {
		Provider<Injector> injectorProvider = getProvider(Injector.class);
		Map<Key<?>, Initializer> initializerCache = new ConcurrentHashMap<>();
		bindListener(new AbstractMatcher<Binding<?>>() {
			@Override
			public boolean matches(Binding<?> binding) {
				return Initializable.class.isAssignableFrom(binding.getKey().getTypeLiteral().getRawType());
			}
		}, new ProvisionListener() {
			@SuppressWarnings("unchecked")
			@Override
			public <T> void onProvision(ProvisionInvocation<T> provision) {
				T object = provision.provision();
				if (object == null) {
					return;
				}
				initializerCache
						.computeIfAbsent(provision.getBinding().getKey(), key -> {
							Type type = key.getTypeLiteral().getType();
							Type initializerType = RecursiveType.of(OptionalInitializer.class, RecursiveType.of(type)).getType();
							Annotation annotation = key.getAnnotation();
							Binding<?> binding = annotation != null ?
									injectorProvider.get().getBinding(Key.get(initializerType, annotation)) :
									injectorProvider.get().getBinding(Key.get(initializerType));
							return (OptionalInitializer) binding.getProvider().get();
						})
						.accept((Initializable) object);
			}
		});
	}
}
