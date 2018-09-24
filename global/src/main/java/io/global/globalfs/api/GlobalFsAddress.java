package io.global.globalfs.api;

import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;

public final class GlobalFsAddress {
	private final GlobalFsName fsName;
	private final String file;

	private GlobalFsAddress(GlobalFsName fsName, String file) {
		this.fsName = fsName;
		this.file = file;
	}

	public static GlobalFsAddress of(PubKey pubKey, String filesystem, String file) {
		return new GlobalFsAddress(GlobalFsName.of(pubKey, filesystem), file);
	}

	public static GlobalFsAddress of(KeyPair keys, String filesystem, String file) {
		return new GlobalFsAddress(GlobalFsName.of(keys.getPubKey(), filesystem), file);
	}

	public static GlobalFsAddress of(PrivKey key, String filesystem, String file) {
		return new GlobalFsAddress(GlobalFsName.of(key.computePubKey(), filesystem), file);
	}

	public GlobalFsAddress addressOf(String file) {
		return new GlobalFsAddress(fsName, file);
	}

	public GlobalFsName getFsName() {
		return fsName;
	}

	public String getFile() {
		return file;
	}

	public PubKey getPubKey() {
		return fsName.getPubKey();
	}

	public String getFilesystem() {
		return fsName.getFsName();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		GlobalFsAddress that = (GlobalFsAddress) o;
		return fsName.equals(that.fsName) && file.equals(that.file);
	}

	@Override
	public int hashCode() {
		return 31 * fsName.hashCode() + file.hashCode();
	}

	@Override
	public String toString() {
		return "GlobalFsAddress{fsName=" + fsName + ", file='" + file + "'}";
	}
}
