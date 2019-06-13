package io.datakernel.di.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Method;

import static io.datakernel.di.util.ReflectionUtils.getShortName;

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
		Class<?> declaringClass = provider.getDeclaringClass();
		String shortName = getShortName(declaringClass.getName());
		return "module " + module + ", provider method "+shortName+"." + provider.getName() + "(" + shortName + ".java:0)";
	}
}
