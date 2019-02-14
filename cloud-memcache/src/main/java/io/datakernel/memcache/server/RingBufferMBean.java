package io.datakernel.memcache.server;

public interface RingBufferMBean {
	void reset();

	String getStatsPuts();

	double getStatsPutsRate();

	long getStatsPutsTotal();

	String getStatsGets();

	double getStatsGetsRate();

	long getStatsGetsTotal();

	String getStatsMisses();

	double getStatsMissesRate();

	long getStatsMissesTotal();

	int getItems();

	long getSize();

	String getCurrentBuffer();

	int getFullCycles();

	String getLifetime();

	long getLifetimeSeconds();
}
