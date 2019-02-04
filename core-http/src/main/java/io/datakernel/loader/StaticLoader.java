package io.datakernel.loader;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.http.HttpException;
import io.datakernel.loader.cache.Cache;

import java.util.function.Predicate;

public interface StaticLoader {

	Promise<ByteBuf> getResource(String name);

	default StaticLoader filter(Predicate<String> predicate) {
		return name -> predicate.test(name) ?
				getResource(name) :
				Promise.ofException(HttpException.notFound404());
	}

	default StaticLoader subfolder(String subfolder) {
		String folder = subfolder.endsWith("/") ? subfolder : subfolder + '/';
		return name -> getResource(folder + name);
	}

	default StaticLoader cached(Cache cache) {
		return new CachedStaticLoader(this, cache);
	}
}
