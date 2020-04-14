package io.datakernel.jmx.api;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target(TYPE)
@Retention(RUNTIME)
public @interface MBeanWrapperFactory {
	Class<? extends JmxWrapperFactory> value();
}
