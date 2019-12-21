package io.global.launchers.sync;

import io.datakernel.async.service.EventloopTaskScheduler;
import io.datakernel.config.Config;
import io.datakernel.di.core.*;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.ModuleBuilder;
import io.datakernel.di.util.Trie;
import io.datakernel.eventloop.Eventloop;
import io.global.kv.GlobalKvNodeImpl;

import java.util.Map;
import java.util.Set;

import static io.datakernel.launchers.initializers.Initializers.ofEventloopTaskScheduler;

public final class KvSyncModule implements Module {
	private final ModuleBuilder builder = Module.create();

	private KvSyncModule() {
	}

	public static KvSyncModule create() {
		return new KvSyncModule();
	}

	public KvSyncModule withCatchUp() {
		builder.bind(Key.of(EventloopTaskScheduler.class).named("KV catchUp"))
				.to((eventloop, node, config) -> EventloopTaskScheduler.create(eventloop, node::catchUp)
								.initialize(ofEventloopTaskScheduler(config.getChild("kv.catchUp"))),
						Eventloop.class, GlobalKvNodeImpl.class, Config.class)
				.asEager();
		return this;
	}

	public KvSyncModule withPush() {
		builder.bind(Key.of(EventloopTaskScheduler.class).named("KV push"))
				.to((eventloop, node, config) -> EventloopTaskScheduler.create(eventloop, node::push)
								.initialize(ofEventloopTaskScheduler(config.getChild("kv.push"))),
						Eventloop.class, GlobalKvNodeImpl.class, Config.class)
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
