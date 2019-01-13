/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.util.guice;

import com.google.inject.*;
import com.google.inject.spi.BindingScopingVisitor;
import com.google.inject.spi.DefaultBindingScopingVisitor;
import io.datakernel.util.RecursiveType;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPoolScope;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class GuiceUtils {
	private GuiceUtils() {
	}

	public static boolean isSingleton(Binding<?> binding) {
		return binding.acceptScopingVisitor(new BindingScopingVisitor<Boolean>() {
			@Override
			public Boolean visitNoScoping() {
				return false;
			}

			@Override
			public Boolean visitScopeAnnotation(Class<? extends Annotation> visitedAnnotation) {
				return visitedAnnotation.equals(Singleton.class);
			}

			@Override
			public Boolean visitScope(Scope visitedScope) {
				return visitedScope.equals(Scopes.SINGLETON);
			}

			@Override
			public Boolean visitEagerSingleton() {
				return true;
			}
		});
	}

	public static String prettyPrintAnnotation(Annotation annotation) {
		StringBuilder sb = new StringBuilder();
		Method[] methods = annotation.annotationType().getDeclaredMethods();
		boolean first = true;
		if (methods.length != 0) {
			for (Method m : methods) {
				try {
					Object value = m.invoke(annotation);
					if (value.equals(m.getDefaultValue()))
						continue;
					String valueStr = value instanceof String ? "\"" + value + "\"" : value.toString();
					String methodName = m.getName();
					if ("value".equals(methodName) && first) {
						sb.append(valueStr);
						first = false;
					} else {
						sb.append(first ? "" : ",").append(methodName).append("=").append(valueStr);
						first = false;
					}
				} catch (ReflectiveOperationException ignored) {
				}
			}
		}
		String simpleName = annotation.annotationType().getSimpleName();
		return "@" + ("NamedImpl".equals(simpleName) ? "Named" : simpleName) + (first ? "" : "(" + sb + ")");
	}

	public static String prettyPrintSimpleKeyName(Key<?> key) {
		Type type = key.getTypeLiteral().getType();
		return (key.getAnnotation() != null ?
				prettyPrintAnnotation(key.getAnnotation()) + " " :
				key.getAnnotationType() != null ?
						"@" + RecursiveType.of(key.getAnnotationType()).getSimpleName() + " " :
						"") + RecursiveType.of(type).getSimpleName();
	}

	public static String prettyPrintKeyName(Key<?> key) {
		Type type = key.getTypeLiteral().getType();
		return (key.getAnnotation() != null ? prettyPrintAnnotation(key.getAnnotation()) + " " : "") +
				RecursiveType.of(type).getName();
	}

	@Nullable
	public static Integer extractWorkerId(Binding<?> binding) {
		return binding.acceptScopingVisitor(new DefaultBindingScopingVisitor<Integer>() {
			@Override
			@Nullable
			public Integer visitScope(Scope scope) {
				return scope instanceof WorkerPoolScope ? ((WorkerPoolScope) scope).getCurrentWorkerId() : null;
			}
		});
	}

	public static WorkerPool extractWorkerPool(Binding<?> binding) {
		return binding.acceptScopingVisitor(new DefaultBindingScopingVisitor<WorkerPool>() {
			@Override
			@Nullable
			public WorkerPool visitScope(Scope scope) {
				return scope instanceof WorkerPoolScope ? ((WorkerPoolScope) scope).getCurrentWorkerPool() : null;
			}
		});
	}

	public interface ModuleWithRemapping extends com.google.inject.Module {
		<T> ModuleWithRemapping remap(Key<T> key, Key<T> to);

		<T> ModuleWithRemapping expose(Key<T> key);
	}

	public static ModuleWithRemapping remapModule(com.google.inject.Module module) {
		return new ModuleWithRemapping() {
			Map<Key<?>, Key<?>> remappedKeys = new LinkedHashMap<>();
			Set<Key<?>> exposedKeys = new LinkedHashSet<>();

			@Override
			public <T> ModuleWithRemapping remap(Key<T> from, Key<T> to) {
				remappedKeys.put(from, to);
				return this;
			}

			@Override
			public <T> ModuleWithRemapping expose(Key<T> key) {
				exposedKeys.add(key);
				return this;
			}

			@SuppressWarnings("AssignmentToNull") // resource release
			@Override
			public void configure(Binder binder) {
				Map<Key<?>, Key<?>> finalRemappedKeys = remappedKeys;
				Set<Key<?>> finalExposedKeys = exposedKeys;
				remappedKeys = null;
				exposedKeys = null;
				binder.install(new PrivateModule() {
					@Override
					protected void configure() {
						install(new PrivateModule() {
							@SuppressWarnings("unchecked")
							@Override
							protected void configure() {
								install(new PrivateModule() {
									@Override
									protected void configure() {
										install(module);
										for (Key<?> key : finalExposedKeys) {
											expose(key);
										}
										for (Key<?> key : finalRemappedKeys.keySet()) {
											expose(key);
										}
									}
								});
								for (Key<?> key : finalExposedKeys) {
									expose(key);
								}
								for (Map.Entry<Key<?>, Key<?>> entry : finalRemappedKeys.entrySet()) {
									bind((Key<Object>) entry.getValue()).to(entry.getKey());
								}
							}
						});
						for (Key<?> key : finalExposedKeys) {
							expose(key);
						}
						for (Key<?> key : finalRemappedKeys.values()) {
							expose(key);
						}
					}
				});
			}
		};
	}
}
