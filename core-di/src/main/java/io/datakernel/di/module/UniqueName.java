package io.datakernel.di.module;

import io.datakernel.di.annotation.NameAnnotation;

@NameAnnotation
public @interface UniqueName {
	// so that this pseudo-annotation is not a 'marker'
	int dummy() default 0;
}
