package io.datakernel.di.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Method;

/**
 * LocationInfo is a transient field in {@link io.datakernel.di.binding.Binding binding} that is set
 * where possible by the DSL so that error messages can show where a binding was made.
 */
public final class LocationInfo {
	private final Object module;
	@Nullable
	private final Method provider;

	private LocationInfo(Object module, @Nullable Method provider) {
		this.module = module;
		this.provider = provider;
	}

	public static LocationInfo from(@NotNull Object module, @NotNull Method provider) {
		return new LocationInfo(module, provider);
	}

	public static LocationInfo from(@NotNull Object module) {
		return new LocationInfo(module, null);
	}

	@NotNull
	public Object getModule() {
		return module;
	}

	@Nullable
	public Method getProvider() {
		return provider;
	}

	@Override
	public String toString() {
		if (provider == null) {
			return "module " + module;
		}
		String shortName = ReflectionUtils.getShortName(provider.getDeclaringClass());
		return "object " + module + ", provider method " + shortName + "." + provider.getName() + "(" + shortName + ".java:0)";
	}
}
