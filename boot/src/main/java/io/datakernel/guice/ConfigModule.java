/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.guice;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import io.datakernel.config.Config;

import java.io.File;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public final class ConfigModule extends AbstractModule {
	private final Config config;
	private final Map<Object, String> bindingToPath = new HashMap<>();

	public ConfigModule(Config config) {
		this.config = checkNotNull(config);
	}

	public ConfigModule(File configFile) {
		this(Config.ofProperties(configFile));
	}

	public ConfigModule bindPath(String named, String path) {
		bindingToPath.put(named, path);
		return this;
	}

	public ConfigModule bindPath(Annotation annotation, String path) {
		bindingToPath.put(annotation, path);
		return this;
	}

	public ConfigModule bindPath(Class<? extends Annotation> annotationType, String path) {
		bindingToPath.put(annotationType, path);
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void configure() {
		bind(Config.class).toInstance(config);

		for (Object binding : bindingToPath.keySet()) {
			String path = bindingToPath.get(binding);
			Config child = config.getChild(path);
			checkNotNull(child, "Config path not found: %s", path);

			if (binding instanceof String) {
				bind(Config.class).annotatedWith(Names.named((String) binding)).toInstance(child);
			} else if (binding instanceof Annotation) {
				bind(Config.class).annotatedWith((Annotation) binding)
						.toInstance(child);
			} else if (binding instanceof Class) {
				bind(Config.class).annotatedWith((Class<? extends Annotation>) binding)
						.toInstance(child);
			}

		}
	}
}
