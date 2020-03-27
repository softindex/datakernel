package io.datakernel.datastream.visitor;

import io.datakernel.common.tuple.Tuple2;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public final class GraphVizStreamVisitor extends StreamVisitor {
	private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";

	private final Set<Transformer> transformers = new HashSet<>();

	private final Map<StreamSupplier<?>, StreamSupplier<?>> forwardedSuppliers = new HashMap<>();
	private final Map<StreamConsumer<?>, StreamConsumer<?>> forwardedConsumers = new HashMap<>();

	private final Set<Object> forwardedForwarders = new HashSet<>();

	private final List<Tuple2<StreamSupplier<?>, StreamConsumer<?>>> links = new ArrayList<>();
	private final List<Tuple2<Object, Object>> implicits = new ArrayList<>();

	private final Map<Object, String> ids = new HashMap<>();
	private final Map<Object, String> clusters = new HashMap<>();
	private final Map<Object, String> labels = new HashMap<>();

	private int idCounter = 0;
	private int clusterCounter = 0;

	@Override
	public void doVisit(StreamSupplier<?> supplier, @Nullable String label) {
		suppliers.add(supplier);
		if (label != null) {
			labels.put(supplier, label);
		}
	}

	@Override
	public void doVisit(StreamConsumer<?> consumer, @Nullable String label) {
		consumers.add(consumer);
		if (label != null) {
			labels.put(consumer, label);
		}
	}

	@Override
	public void visitForwarder(StreamSupplier<?> wrapping, StreamSupplier<?> peer) {
		if (forwardedSuppliers.containsKey(peer)) {
			forwardedForwarders.add(peer);
		}
		forwardedSuppliers.put(wrapping, peer);
		clusters.put(wrapping, "cluster" + clusterCounter++);
	}

	@Override
	public void visitForwarder(StreamConsumer<?> wrapping, StreamConsumer<?> peer) {
		forwardedConsumers.put(wrapping, peer);
		if (forwardedConsumers.containsKey(peer)) {
			forwardedForwarders.add(peer);
		}
		clusters.put(wrapping, "cluster" + clusterCounter++);
	}

	@Override
	public void visitStream(StreamSupplier<?> from, StreamConsumer<?> to) {
		links.add(new Tuple2<>(from, to));
	}

	@Override
	public void visitTransformer(List<? extends StreamConsumer<?>> inputs, List<? extends StreamSupplier<?>> outputs, String label) {
		Transformer tf = new Transformer(inputs, outputs);
		transformers.add(tf);
		labels.put(tf, label);
	}

	@Override
	public void visitImplicit(Object from, Object to) {
		implicits.add(new Tuple2<>(from, to));
	}

	public String toGraphViz() {
		StringBuilder sb = new StringBuilder("digraph {\n  compound=true;\n  node[shape=box, style=rounded];\n\n");

		suppliers.forEach(s -> {
			if (!forwardedSuppliers.containsKey(s)) {
				sb.append("  ").append(id(s)).append("[label=\"").append(label(s)).append("\", color=blue]").append(";\n");
			}
		});
		consumers.forEach(s -> {
			if (!forwardedConsumers.containsKey(s)) {
				sb.append("  ").append(id(s)).append("[label=\"").append(label(s)).append("\", color=red]").append(";\n");
			}
		});

		if (!forwardedSuppliers.isEmpty()) {
			sb.append('\n');
		}
		buildForwarders(sb, forwardedSuppliers);

		if (!forwardedConsumers.isEmpty()) {
			sb.append('\n');
		}
		buildForwarders(sb, forwardedConsumers);

		if (!links.isEmpty()) {
			sb.append('\n');
		}
		links.forEach(link -> {
			StreamSupplier<?> supplier = link.getValue1();
			StreamConsumer<?> consumer = link.getValue2();
			if (!suppliers.contains(supplier) || !consumers.contains(consumer)) {
				return;
			}
			sb.append("  ").append(id(getLeaf(supplier))).append(" -> ").append(id(getLeaf(consumer)));
			List<String> args = new ArrayList<>();
			String cluster = clusters.get(supplier);
			if (cluster != null) {
				args.add("ltail=" + cluster);
			}
			cluster = clusters.get(consumer);
			if (cluster != null) {
				args.add("lhead=" + cluster);
			}
			if (args.size() != 0) {
				sb.append(" [").append(String.join(", ", args)).append("];\n");
			} else {
				sb.append(";\n");
			}
		});

		if (!transformers.isEmpty()) {
			sb.append('\n');
		}
		transformers.forEach(tf -> {
			sb.append("  ").append(id(tf)).append(" [label=\"").append(label(tf)).append("\", shape=oval, color=gray];\n");
			String tfId = id(tf);
			tf.inputs.forEach(input -> {
				if (!consumers.contains(input)) {
					return;
				}
				sb.append("  ").append(id(getLeaf(input))).append(" -> ").append(tfId);
				String cluster = clusters.get(input);
				if (cluster != null) {
					sb.append(" [lhead=").append(cluster).append("];\n");
				} else {
					sb.append(";\n");
				}
			});
			tf.outputs.forEach(supplier -> {
				if (!suppliers.contains(supplier)) {
					return;
				}
				sb.append("  ").append(tfId).append(" -> ").append(id(getLeaf(supplier)));
				String cluster = clusters.get(supplier);
				if (cluster != null) {
					sb.append(" [ltail=").append(cluster).append("];\n");
				} else {
					sb.append(";\n");
				}
			});
		});

		if (!implicits.isEmpty()) {
			sb.append('\n');
		}

		implicits.forEach(link -> {
			Object start = link.getValue1();
			Object end = link.getValue2();
			sb.append("  ").append(id(getLeaf(start))).append(" -> ").append(id(getLeaf(end))).append(" [style=dashed");
			String cluster = clusters.get(start);
			if (cluster != null) {
				sb.append(", ltail=").append(cluster);
			}
			cluster = clusters.get(end);
			if (cluster != null) {
				sb.append(", lhead=").append(cluster);
			}
			sb.append("];\n");
		});

		return sb.append("}").toString();
	}

	private void buildForwarders(StringBuilder sb, Map<?, ?> forwarders) {
		forwarders.forEach((wrapper, peer) -> {
			if (forwardedForwarders.contains(wrapper)) {
				return;
			}
			int depth = 0;

			Object last = wrapper;
			Object x = peer;
			do {
				++depth;
				for (int i = 0; i < depth; i++) {
					sb.append("  ");
				}
				sb.append("subgraph ")
						.append(clusters.get(last))
						.append(" {\n");
				for (int i = 0; i <= depth; i++) {
					sb.append("  ");
				}
				sb.append("label=\"")
						.append(label(last))
						.append("\";\n");
				for (int i = 0; i <= depth; i++) {
					sb.append("  ");
				}
				sb.append("style=rounded;\n");
				last = x;
			} while ((x = forwarders.get(x)) != null);

			for (int i = 0; i <= depth; i++) {
				sb.append("  ");
			}
			sb.append(id(last)).append(";\n");

			for (int i = 0; i < depth; i++) {
				for (int j = 0; j < depth - i; j++) {
					sb.append("  ");
				}
				sb.append("}\n");
			}
		});
	}

	private String id(Object object) {
		return ids.computeIfAbsent(object, obj -> {
			int id = ++idCounter;
			StringBuilder sb = new StringBuilder();
			while (id != 0) {
				sb.append(ALPHABET.charAt((id - 1) % ALPHABET.length()));
				id /= ALPHABET.length();
			}
			return sb.toString();
		});
	}

	private String label(Object object) {
		return labels.computeIfAbsent(object, obj -> {
			if (object instanceof StreamSupplier) {
				String label = ((StreamSupplier<?>) object).getLabel();
				if (label != null) {
					return label;
				}
			}
			if (object instanceof StreamConsumer) {
				String label = ((StreamConsumer<?>) object).getLabel();
				if (label != null) {
					return label;
				}
			}
			return getShortClassName(object.getClass());
		});
	}

	private StreamSupplier<?> getLeaf(StreamSupplier<?> supplier) {
		StreamSupplier<?> peer = supplier;
		while ((peer = forwardedSuppliers.get(peer)) != null) {
			supplier = peer;
		}
		return supplier;
	}

	private StreamConsumer<?> getLeaf(StreamConsumer<?> consumer) {
		StreamConsumer<?> peer = consumer;
		while ((peer = forwardedConsumers.get(peer)) != null) {
			consumer = peer;
		}
		return consumer;
	}

	private Object getLeaf(Object visitable) {
		if (visitable instanceof StreamSupplier) {
			return getLeaf((StreamSupplier<?>) visitable);
		}
		if (visitable instanceof StreamConsumer) {
			return getLeaf((StreamConsumer<?>) visitable);
		}
		return visitable;
	}

	private static final class Transformer {
		final List<? extends StreamConsumer<?>> inputs;
		final List<? extends StreamSupplier<?>> outputs;

		private Transformer(List<? extends StreamConsumer<?>> inputs, List<? extends StreamSupplier<?>> outputs) {
			this.inputs = inputs;
			this.outputs = outputs;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Transformer that = (Transformer) o;
			return inputs.equals(that.inputs) && outputs.equals(that.outputs);
		}

		@Override
		public int hashCode() {
			return 31 * inputs.hashCode() + outputs.hashCode();
		}
	}
}
