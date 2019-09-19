package io.global.comm.pojo;

public final class IpBanState {
	private final BanState banState;
	private final IpRange ipRange;

	public IpBanState(BanState state, IpRange range) {
		banState = state;
		ipRange = range;
	}

	public BanState getBanState() {
		return banState;
	}

	public IpRange getIpRange() {
		return ipRange;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		IpBanState state = (IpBanState) o;

		if (!banState.equals(state.banState)) return false;
		return ipRange.equals(state.ipRange);
	}

	@Override
	public int hashCode() {
		int result = banState.hashCode();
		result = 31 * result + ipRange.hashCode();
		return result;
	}
}
