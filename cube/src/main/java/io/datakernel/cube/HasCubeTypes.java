package io.datakernel.cube;

import java.lang.reflect.Type;
import java.util.Set;

public interface HasCubeTypes {
	Set<String> getAttributes();

	Type getAttributeType(String key);

	Set<String> getMeasures();

	Type getMeasureType(String field);
}
