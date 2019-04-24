package io.datakernel.examples;

import java.util.List;
import java.util.Map;

public interface RecordDAO {
	void add(Record record);

	void delete(int id);

	Record find(int id);

	Map<Integer, Record> findAll();
}
