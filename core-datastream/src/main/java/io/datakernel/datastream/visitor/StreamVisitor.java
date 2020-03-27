package io.datakernel.datastream.visitor;

import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;

public abstract class StreamVisitor {
	protected Set<StreamSupplier<?>> suppliers = new HashSet<>();
	protected Set<StreamConsumer<?>> consumers = new HashSet<>();

	public final boolean unseen(StreamSupplier<?> supplier) {
		return !suppliers.contains(supplier);
	}

	public final boolean unseen(StreamConsumer<?> consumer) {
		return !consumers.contains(consumer);
	}

	public final void visit(StreamSupplier<?> supplier, @Nullable String label) {
		suppliers.add(supplier);
		doVisit(supplier, label);
	}

	public final void visit(StreamSupplier<?> supplier) {
		visit(supplier, null);
	}

	public final void visit(StreamConsumer<?> consumer, @Nullable String label) {
		consumers.add(consumer);
		doVisit(consumer, label);
	}

	public final void visit(StreamConsumer<?> consumer) {
		visit(consumer, null);
	}

	protected void doVisit(StreamSupplier<?> supplier, @Nullable String label) {
	}

	protected void doVisit(StreamConsumer<?> consumer, @Nullable String label) {
	}

	public void visitForwarder(StreamSupplier<?> wrapping, StreamSupplier<?> peer) {
	}

	public void visitForwarder(StreamConsumer<?> wrapping, StreamConsumer<?> peer) {
	}

	public void visitStream(StreamSupplier<?> supplier, StreamConsumer<?> consumer) {
	}

	public void visitTransformer(List<? extends StreamConsumer<?>> inputs, List<? extends StreamSupplier<?>> outputs, String label) {
	}

	// region public void visitTransformer(...) { ... } // shortcuts
	public final void visitTransformer(StreamConsumer<?> input, StreamSupplier<?> output, String label) {
		visitTransformer(singletonList(input), singletonList(output), label);
	}

	public final void visitTransformer(List<? extends StreamConsumer<?>> inputs, StreamSupplier<?> output, String label) {
		visitTransformer(inputs, singletonList(output), label);
	}

	public final void visitTransformer(StreamConsumer<?> input, List<? extends StreamSupplier<?>> outputs, String label) {
		visitTransformer(singletonList(input), outputs, label);
	}

	public final void visitTransformer(List<? extends StreamConsumer<?>> inputs, List<? extends StreamSupplier<?>> outputs, Class<?> transformerClass) {
		visitTransformer(inputs, outputs, getShortClassName(transformerClass));
	}

	public final void visitTransformer(StreamConsumer<?> input, StreamSupplier<?> output, Class<?> transformerClass) {
		visitTransformer(singletonList(input), singletonList(output), getShortClassName(transformerClass));
	}

	public final void visitTransformer(List<? extends StreamConsumer<?>> inputs, StreamSupplier<?> output, Class<?> transformerClass) {
		visitTransformer(inputs, singletonList(output), getShortClassName(transformerClass));
	}

	public final void visitTransformer(StreamConsumer<?> input, List<? extends StreamSupplier<?>> outputs, Class<?> transformerClass) {
		visitTransformer(singletonList(input), outputs, getShortClassName(transformerClass));
	}
	// endregion

	public void visitImplicit(Object from, Object to) {
	}

	protected static String getShortClassName(Class<?> cls) {
		return cls.getName()
				.replaceAll("(?:\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*\\.)*", "");
	}
}
