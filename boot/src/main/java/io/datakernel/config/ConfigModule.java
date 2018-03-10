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
import io.datakernel.service.BlockingService;
import io.datakernel.service.ServiceGraph;
import io.datakernel.util.Initializable;
import io.datakernel.util.Initializer;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.util.guice.RequiredDependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;

public final class ConfigModule extends AbstractModule implements Initializable<ConfigModule> {
	private static final Logger logger = LoggerFactory.getLogger(ConfigModule.class);

	private Supplier<Config> configSupplier;
	private Path effectiveConfigPath;
	private Path finalConfigPath;

	private interface ConfigSaveService extends BlockingService {
	}

	private ConfigModule(Supplier<Config> configSupplier) {
		this.configSupplier = configSupplier;
	}

	public static ConfigModule create(Supplier<Config> configSupplier) {
		return new ConfigModule(configSupplier);
	}

	public static ConfigModule create(Config config) {
		return new ConfigModule(() -> config);
	}

	public ConfigModule saveEffectiveConfigTo(String file) {
		return saveEffectiveConfigTo(Paths.get(file));
	}

	public ConfigModule saveEffectiveConfigTo(Path file) {
		this.effectiveConfigPath = file;
		return this;
	}

	public ConfigModule saveFinalConfigTo(String file) {
		return saveFinalConfigTo(Paths.get(file));
	}

	public ConfigModule saveFinalConfigTo(Path file) {
		this.finalConfigPath = file;
		return this;
	}

	@Override
	protected void configure() {
		bind(new TypeLiteral<RequiredDependency<ServiceGraph>>() {}).asEagerSingleton();
		bind(new TypeLiteral<RequiredDependency<ConfigSaveService>>() {}).asEagerSingleton();
	}

	@Provides
	@Singleton
	Config provideConfig() {
		Config config = configSupplier.get();
		return effectiveConfigPath != null || finalConfigPath != null ? EffectiveConfig.wrap(config) : config;
	}

	@Provides
	@Singleton
	ConfigSaveService configSaveService(Config config,
	                                    OptionalDependency<Initializer<ConfigModule>> maybeInitializer) {
		maybeInitializer.ifPresent(initializer -> initializer.accept(this));
		return new ConfigSaveService() {
			@Override
			public void start() throws Exception {
				if (effectiveConfigPath != null && config instanceof EffectiveConfig) {
					logger.info("Saving effective config to {}", effectiveConfigPath);
					((EffectiveConfig) config).saveEffectiveConfigTo(effectiveConfigPath);
				}
			}

			@Override
			public void stop() throws Exception {
				if (finalConfigPath != null && config instanceof EffectiveConfig) {
					logger.info("Saving final config to {}", finalConfigPath);
					((EffectiveConfig) config).saveEffectiveConfigTo(finalConfigPath);
				}
			}
		};
	}

}