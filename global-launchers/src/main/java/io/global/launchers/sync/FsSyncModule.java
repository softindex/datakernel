package io.global.launchers.sync;

import io.datakernel.async.service.EventloopTaskScheduler;
import io.datakernel.config.Config;
import io.datakernel.di.core.*;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.ModuleBuilder;
import io.datakernel.di.util.Trie;
import io.datakernel.eventloop.Eventloop;
import io.global.fs.local.GlobalFsNodeImpl;

import java.util.Map;
import java.util.Set;

import static io.datakernel.launchers.initializers.Initializers.ofEventloopTaskScheduler;

public final class FsSyncModule implements Module {
	private final ModuleBuilder builder = Module.create();

	private FsSyncModule() {
	}

	public static FsSyncModule create() {
		return new FsSyncModule();
	}

	public FsSyncModule withCatchUp() {
		builder.bind(Key.of(EventloopTaskScheduler.class).named("FS catchUp"))
				.to((eventloop, node, config) -> EventloopTaskScheduler.create(eventloop, node::catchUp)
								.initialize(ofEventloopTaskScheduler(config.getChild("fs.catchUp"))),
						Eventloop.class, GlobalFsNodeImpl.class, Config.class)
				.asEager();
		return this;
	}

	public FsSyncModule withPush() {
		builder.bind(Key.of(EventloopTaskScheduler.class).named("FS push"))
				.to((eventloop, node, config) -> EventloopTaskScheduler.create(eventloop, node::push)
								.initialize(ofEventloopTaskScheduler(config.getChild("fs.push"))),
						Eventloop.class, GlobalFsNodeImpl.class, Config.class)
				.asEager();
		return this;
	}

	public FsSyncModule withFetch(String glob) {
		builder.bind(Key.of(EventloopTaskScheduler.class).named("FS fetch"))
				.to((eventloop, node, config) -> EventloopTaskScheduler.create(eventloop, () -> node.fetch(glob))
								.initialize(ofEventloopTaskScheduler(config.getChild("fs.fetch"))),
						Eventloop.class, GlobalFsNodeImpl.class, Config.class)
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
