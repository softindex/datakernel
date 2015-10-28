package io.datakernel.rpc.hash;

public interface BucketHashFunction {
	int hash(Object key, int bucket);
}
