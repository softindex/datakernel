package io.global.crdt;

import io.global.common.Hash;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

public final class RawCrdtData {
	private final byte[] key;
	private final byte[] value;

	@Nullable
	private Hash simKeyHash;

	private RawCrdtData(byte[] key, byte[] value, @Nullable Hash simKeyHash) {
		this.key = key;
		this.value = value;
		this.simKeyHash = simKeyHash;
	}

	public static RawCrdtData of(byte[] key, byte[] value, @Nullable Hash simKeyHash) {
		return new RawCrdtData(key, value, simKeyHash);
	}

	public static RawCrdtData parse(byte[] key, byte[] value, @Nullable Hash simKeyHash) {
		return new RawCrdtData(key, value, simKeyHash);
	}

	public byte[] getKey() {
		return key;
	}

	public byte[] getValue() {
		return value;
	}

	@Nullable
	public Hash getSimKeyHash() {
		return simKeyHash;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		RawCrdtData that = (RawCrdtData) o;

		return Arrays.equals(key, that.key) && Arrays.equals(value, that.value) && Objects.equals(simKeyHash, that.simKeyHash);
	}

	@Override
	public int hashCode() {
		return 31 * (31 * Arrays.hashCode(key) + Arrays.hashCode(value)) + (simKeyHash != null ? simKeyHash.hashCode() : 0);
	}
}
