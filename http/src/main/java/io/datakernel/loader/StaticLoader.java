package io.datakernel.loader;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.http.HttpException;
import io.datakernel.loader.cache.Cache;

import java.util.function.Predicate;

public interface StaticLoader {

	Stage<ByteBuf> getResource(String name);

	default StaticLoader filter(Predicate<String> predicate) {
		return name -> predicate.test(name) ?
				getResource(name) :
				Stage.ofException(HttpException.notFound404());
	}

	default StaticLoader cached(Cache cache) {
		return new CachedStaticLoader(this, cache);
	}
}
