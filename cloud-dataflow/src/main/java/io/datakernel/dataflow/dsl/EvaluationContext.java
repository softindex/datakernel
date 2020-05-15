package io.datakernel.dataflow.dsl;

import io.datakernel.dataflow.dataset.Dataset;
import io.datakernel.dataflow.graph.DataflowGraph;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class EvaluationContext {
	private final List<String> prefixes = new ArrayList<>();
	private final Map<String, Dataset<?>> environment = new HashMap<>();

	private final DataflowGraph graph;

	public EvaluationContext(DataflowGraph graph) {
		this.graph = graph;
		prefixes.add("java.lang");
	}

	public void addPrefix(String prefix) {
		prefixes.add(prefix);
	}

	public void put(String name, Dataset<?> dataset) {
		environment.put(name, dataset);
	}

	public Dataset<?> get(String name) {
		return environment.get(name);
	}

	public DataflowGraph getGraph() {
		return graph;
	}

	@SuppressWarnings("unchecked")
	public <T> Class<T> resolveClass(String s) {
		for (String prefix : prefixes) {
			try {
				if (prefix.endsWith("$")) {
					return (Class<T>) Class.forName(prefix + s);
				} else {
					return (Class<T>) Class.forName(prefix + "." + s);
				}
			} catch (ClassNotFoundException ignored) {
			}
		}
		try {
			return (Class<T>) Class.forName(s);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Failed to resolve class " + s);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T generateInstance(String s) {
		Class<Object> cls = resolveClass(s);
		try {
			Constructor<?> constructor = cls.getDeclaredConstructor();
			constructor.setAccessible(true);
			return (T) constructor.newInstance();
		} catch (ReflectiveOperationException e) {
			throw new IllegalArgumentException("Failed to generate an instance for class " + s);
		}
	}
}
