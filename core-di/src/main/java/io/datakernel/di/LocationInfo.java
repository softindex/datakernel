package io.datakernel.di;

import org.jetbrains.annotations.Nullable;

public final class LocationInfo {
	@Nullable
	private final Class<?> moduleClass;
	private final String declaration;

	public LocationInfo(@Nullable Class<?> moduleClass, String declaration) {
		this.moduleClass = moduleClass;
		this.declaration = declaration;
	}

	@Nullable
	public Class<?> getModuleClass() {
		return moduleClass;
	}

	public String getDeclaration() {
		return declaration;
	}

	@Override
	public String toString() {
		return declaration;
	}
}
