package io.datakernel.docs.dao;

import java.io.IOException;

public interface ResourceDao {
	String getResource(String resourceName) throws IOException;
	boolean exist(String resourceName);
}
