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

package io.datakernel.config;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import io.datakernel.config.impl.ConfigWithFullPath;
import io.datakernel.service.BlockingService;
import io.datakernel.service.ServiceGraph;
import io.datakernel.util.Initializer;
import io.datakernel.util.guice.RequiredDependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class ConfigsModule extends AbstractModule implements Initializer<ConfigsModule> {
	private static final Logger logger = LoggerFactory.getLogger(ConfigsModule.class);

	private Config config;
	private Path saveFile;

	// creators & builders
	private ConfigsModule(Config config) {
		this.config = config;
	}

	public static ConfigsModule create(Config config) {
		return new ConfigsModule(ConfigWithFullPath.wrap(config));
	}

	public ConfigsModule saveEffectiveConfigTo(String file) {
		return saveEffectiveConfigTo(Paths.get(file));
	}

	public ConfigsModule saveEffectiveConfigTo(Path file) {
		this.saveFile = file;
		return this;
	}

	@Override
	protected void configure() {
		if (saveFile != null) {
			config = EffectiveConfig.create(config);
		}
		bind(new TypeLiteral<RequiredDependency<ServiceGraph>>() {}).asEagerSingleton();
		bind(new TypeLiteral<RequiredDependency<ConfigSaveService>>() {}).asEagerSingleton();
	}

	private interface ConfigSaveService extends BlockingService {
	}

	@Provides
	@Singleton
	ConfigSaveService configSaveService() {
		return new ConfigSaveService() {
			@Override
			public void start() throws Exception {
				logger.info("Saving resulting config to {}", saveFile);
				if (config instanceof EffectiveConfig) {
					((EffectiveConfig) config).saveEffectiveConfig(saveFile);
				}
			}

			@Override
			public void stop() throws Exception {
			}
		};
	}

	@Provides
	@Singleton
	Config provideConfig() {
		return config;
	}
}