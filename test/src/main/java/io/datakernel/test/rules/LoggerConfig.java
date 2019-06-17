package io.datakernel.test.rules;

import java.lang.annotation.*;

@Repeatable(LoggerConfig.Container.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface LoggerConfig {
	Class<?> logger() default Void.class;

	Class<?> packageOf() default Void.class;

	LoggerLevel value();

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ElementType.METHOD, ElementType.TYPE})
	@interface Container {
		LoggerConfig[] value();
	}
}
