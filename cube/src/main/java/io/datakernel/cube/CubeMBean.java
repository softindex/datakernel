package io.datakernel.cube;

public interface CubeMBean {
	void setIgnoreChunkReadingExceptions(boolean ignoreChunkReadingExceptions);

	boolean isIgnoreChunkReadingExceptions();

	void setChunkSize(int chunkSize);

	int getChunkSize();

	void setSorterItemsInMemory(int sorterItemsInMemory);

	int getSorterItemsInMemory();
}
