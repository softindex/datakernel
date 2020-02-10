package io.global.forum.http.view;

import io.datakernel.common.tuple.Tuple4;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.global.comm.dao.CommDao;
import io.global.comm.pojo.BanState;
import io.global.comm.pojo.IpBanState;
import io.global.comm.pojo.IpRange;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public final class IpBanView {
	private final String id;
	private final Tuple4<String, String, String, String> ip;
	private final Tuple4<String, String, String, String> mask;
	private final BanView ban;

	private IpBanView(String id, Tuple4<String, String, String, String> ip, Tuple4<String, String, String, String> mask, BanView ban) {
		this.id = id;
		this.ip = ip;
		this.mask = mask;
		this.ban = ban;
	}

	public String getBanId() {
		return id;
	}

	public Tuple4<String, String, String, String> getIpParts() {
		return ip;
	}

	public Tuple4<String, String, String, String> getMaskParts() {
		return mask;
	}

	public String getIp() {
		return ip.getValue1() + '.' + ip.getValue2() + '.' + ip.getValue3() + '.' + ip.getValue4();
	}

	public String getMask() {
		return mask.getValue1() + '.' + mask.getValue2() + '.' + mask.getValue3() + '.' + mask.getValue4();
	}

	public BanView getBan() {
		return ban;
	}

	public static Promise<IpBanView> from(CommDao commDao, String id) {
		return commDao.getIpBans().get(id)
				.then(banState -> banState == null ? Promise.of(null) : from(commDao, id, banState));
	}

	public static Promise<IpBanView> from(CommDao commDao, String id, IpBanState state) {
		BanState banState = state.getBanState();
		IpRange range = state.getIpRange();
		return BanView.from(commDao, banState)
				.map(ban -> {
					if (ban == null) {
						return null;
					}
					return new IpBanView(id, convert(range.getIp()), convert(range.getMask()), ban);
				});
	}

	private static Tuple4<String, String, String, String> convert(byte[] bytes) {
		return new Tuple4<>("" + (bytes[0] & 0xFF), "" + (bytes[1] & 0xFF), "" + (bytes[2] & 0xFF), "" + (bytes[3] & 0xFF));
	}

	public static Promise<List<IpBanView>> from(CommDao commDao, int page, int limit) {
		return commDao.getIpBans().slice(page * limit, limit)
				.then(bannedIps -> Promises.toList(bannedIps.stream().map(e -> from(commDao, e.getKey(), e.getValue()))))
				.map(list -> list.stream()
						.filter(Objects::nonNull)
						.sorted(Comparator.comparing(ibv -> ibv.ban.getUntilInstant()))
						.collect(toList()));
	}
}
