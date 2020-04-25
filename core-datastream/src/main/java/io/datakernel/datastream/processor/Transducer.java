package io.datakernel.datastream.processor;

import io.datakernel.datastream.StreamDataAcceptor;

import java.util.function.Function;
import java.util.function.Predicate;

public interface Transducer<I, O, A> {
    A onStarted(StreamDataAcceptor<O> output);

    void onItem(StreamDataAcceptor<O> output, I item, A accumulator);

    void onEndOfStream(StreamDataAcceptor<O> output, A accumulator);

	boolean isOneToMany();

    static <T> Transducer<T, T, Void> filter(Predicate<? super T> predicate) {
        return new AbstractTransducer<T, T, Void>() {
            @Override
            public void onItem(StreamDataAcceptor<T> output, T item, Void accumulator) {
                if (predicate.test(item)) {
                    output.accept(item);
                }
            }
        };
    }

    static <I, O> Transducer<I, O, Void> mapper(Function<? super I, ? extends O> fn) {
        return new AbstractTransducer<I, O, Void>() {
            @Override
            public void onItem(StreamDataAcceptor<O> output, I item, Void accumulator) {
				output.accept(fn.apply(item));
            }
        };
    }

}
