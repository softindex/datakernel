package io.global.launchers.sync;

import io.datakernel.async.service.EventloopTaskScheduler;
import io.datakernel.config.Config;
import io.datakernel.di.core.*;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.ModuleBuilder;
import io.datakernel.di.util.Trie;
import io.datakernel.eventloop.Eventloop;
import io.global.pm.GlobalPmNodeImpl;

import java.util.Map;
import java.util.Set;

import static io.datakernel.launchers.initializers.Initializers.ofEventloopTaskScheduler;

public final class PmSyncModule implements Module {
	private final ModuleBuilder builder = Module.create();

	private PmSyncModule() {
	}

	public static PmSyncModule create() {
		return new PmSyncModule();
	}

	public PmSyncModule withCatchUp() {
		builder.bind(Key.of(EventloopTaskScheduler.class).named("PM catchUp"))
				.to((eventloop, node, config) -> EventloopTaskScheduler.create(eventloop, node::catchUp)
								.initialize(ofEventloopTaskScheduler(config.getChild("pm.catchUp"))),
						Eventloop.class, GlobalPmNodeImpl.class, Config.class)
				.asEager();
		return this;
	}

	public PmSyncModule withPush() {
		builder.bind(Key.of(EventloopTaskScheduler.class).named("PM push"))
				.to((eventloop, node, config) -> EventloopTaskScheduler.create(eventloop, node::push)
								.initialize(ofEventloopTaskScheduler(config.getChild("pm.push"))),
						Eventloop.class, GlobalPmNodeImpl.class, Config.class)
				.asEager();
		return this;
	}

	public PmSyncModule withFetch(String mailbox) {
		builder.bind(Key.of(EventloopTaskScheduler.class).named("PM fetch"))
				.to((eventloop, node, config) -> EventloopTaskScheduler.create(eventloop, () -> node.fetch(mailbox))
								.initialize(ofEventloopTaskScheduler(config.getChild("pm.fetch"))),
						Eventloop.class, GlobalPmNodeImpl.class, Config.class)
				.asEager();
		return this;
	}

	@Override
	public Trie<Scope, Map<Key<?>, BindingSet<?>>> getBindings() {
		return builder.getBindings();
	}

	@Override
	public Map<Integer, Set<BindingTransformer<?>>> getBindingTransformers() {
		return builder.getBindingTransformers();
	}

	@Override
	public Map<Class<?>, Set<BindingGenerator<?>>> getBindingGenerators() {
		return builder.getBindingGenerators();
	}

	@Override
	public Map<Key<?>, Multibinder<?>> getMultibinders() {
		return builder.getMultibinders();
	}
}
