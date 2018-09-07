package io.global.globalfs.api;

public interface CheckpointPositionStrategy {

	long nextPosition(long prevPosition);
}
