package io.datakernel.di.annotation;

import io.datakernel.di.core.Key;
import io.datakernel.di.core.Qualifier;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This is a marker meta-annotation that declares an annotation as the one that
 * can be used as a {@link Key key} qualifier.
 * <p>
 * Creating a custom stateless qualifier annotation is as easy as creating your own annotation with no parameters
 * and annotating it with {@link QualifierAnnotation}. That way, an actual class of your stateless qualifier annotation
 * will be used as a qualifier.
 * <p>
 * If you want to create a stateful annotation, you should also annotate it with {@link QualifierAnnotation}.
 * Additionally, you need to get an instance of it with compatible equals method (you can use {@link Qualifier}.NamedImpl class as an example)
 * After that, you can use your annotation in our DSL's and then make keys programmatically with created qualifier instances.
 */
@Target(ANNOTATION_TYPE)
@Retention(RUNTIME)
public @interface QualifierAnnotation {
}
