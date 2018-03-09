package io.datakernel.util.guice;

import com.google.inject.AbstractModule;

/**
 * Guice module which does not require to implement configure method.
 * Also it can be just instantiated if an empty or a stub module is required.
 */
public class SimpleModule extends AbstractModule {
	@Override
	protected void configure() {
	}
}
