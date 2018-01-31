package io.datakernel.loader;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.http.HttpException;

import java.util.function.Predicate;

class PredicateNamesStaticLoader implements StaticLoader {
    private final Predicate<String> names;
    private final StaticLoader loader;

    public PredicateNamesStaticLoader(StaticLoader loader, Predicate<String> names) {
        this.names = names;
        this.loader = loader;
    }

    @Override
    public Stage<ByteBuf> getResource(String name) {
        return names.test(name)
                ? loader.getResource(name)
                : Stage.ofException(HttpException.notFound404());

    }
}