package io.datakernel.logger.config;

import io.datakernel.logger.LoggerConfigurer;

public final class SLF4JLoggingConfig {
	public SLF4JLoggingConfig() {
		LoggerConfigurer.enableSLF4Jbridge();
	}
}
