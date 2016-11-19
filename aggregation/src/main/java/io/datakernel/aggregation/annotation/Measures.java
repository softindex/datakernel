package io.datakernel.aggregation.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({FIELD, METHOD, TYPE})
@Retention(RUNTIME)
public @interface Measures {
	String[] value();
}
