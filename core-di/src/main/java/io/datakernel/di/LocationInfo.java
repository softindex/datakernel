package io.datakernel.di;

import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static io.datakernel.di.util.ReflectionUtils.getShortName;

public final class LocationInfo {
	@Nullable
	private final Class<?> moduleClass;
	private final String declaration;

	private LocationInfo(@Nullable Class<?> moduleClass, String declaration) {
		this.moduleClass = moduleClass;
		this.declaration = declaration;
	}

	public static LocationInfo from(Method providerMethod) {
		Class<?> declaringClass = providerMethod.getDeclaringClass();
		Class<?> module = Modifier.isStatic(providerMethod.getModifiers()) ? null : declaringClass;
		return new LocationInfo(module, declaringClass.getName() + "." + providerMethod.getName() + "(" + getShortName(declaringClass) + ".java:0)");
	}

	public static LocationInfo from(Constructor<?> injectConstructor) {
		Class<?> declaringClass = injectConstructor.getDeclaringClass();
		return new LocationInfo(null, declaringClass.getName() + ".<init>(" + getShortName(declaringClass) + ".java:0)");
	}

	public static LocationInfo from(StackTraceElement binderCall) {
		try {
			return new LocationInfo(Class.forName(binderCall.getClassName()), binderCall.toString());
		} catch (ClassNotFoundException e) {
			return new LocationInfo(null, binderCall.toString());
		}
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
		return declaration + (moduleClass != null ? " from module " + moduleClass.getSimpleName() : "");
	}
}
