package io.global.common;

import java.math.BigInteger;

public final class ECDSASignature {
	public final BigInteger r, s;

	public ECDSASignature(BigInteger r, BigInteger s) {
		this.r = r;
		this.s = s;
	}

	public boolean isCanonical() {
		return s.compareTo(CryptoUtils.HALF_CURVE_ORDER) <= 0;
	}

	public ECDSASignature toCanonicalised() {
		if (!isCanonical()) {
			return new ECDSASignature(r, CryptoUtils.CURVE.getN().subtract(s));
		} else {
			return this;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ECDSASignature that = (ECDSASignature) o;
		return r.equals(that.r) && s.equals(that.s);
	}

	@Override
	public int hashCode() {
		return 31 * r.hashCode() + s.hashCode();
	}
}
