package io.global.globalsync.http;

import com.google.gson.TypeAdapter;
import io.global.common.CryptoUtils;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.globalsync.api.CommitId;
import io.global.globalsync.api.RawCommit;
import io.global.globalsync.api.RawCommitHead;
import io.global.globalsync.api.RepositoryName;
import io.global.globalsync.api.RawServer.HeadsDelta;
import io.global.globalsync.api.RawServer.HeadsInfo;
import io.datakernel.util.gson.GsonAdapters;
import org.spongycastle.math.ec.ECPoint;

import java.math.BigInteger;
import java.util.Base64;
import java.util.Map;
import java.util.Set;

import static io.datakernel.http.HttpUtils.urlDecode;
import static io.datakernel.http.HttpUtils.urlEncode;
import static io.datakernel.util.gson.GsonAdapters.BYTES_JSON;
import static io.datakernel.util.gson.GsonAdapters.STRING_JSON;
import static java.util.stream.Collectors.toMap;

public class HttpDataFormats {
	private HttpDataFormats() {}

	public static final String SAVE = "save";
	public static final String LOAD_COMMIT = "loadCommit";
	public static final String GET_HEADS_INFO = "getHeadsInfo";
	public static final String SAVE_SNAPSHOT = "saveSnapshot";
	public static final String LOAD_SNAPSHOT = "loadSnapshot";
	public static final String GET_HEADS = "getHeads";
	public static final String SHARE_KEY = "shareKey";

	public static final TypeAdapter<Set<String>> SET_OF_STRINGS = GsonAdapters.ofSet(STRING_JSON);

	public static final TypeAdapter<SignedData<RawCommitHead>> COMMIT_HEAD_JSON = GsonAdapters.transform(BYTES_JSON,
			bytes -> SignedData.ofBytes(bytes, RawCommitHead::ofBytes),
			SignedData::toBytes);

	public static final TypeAdapter<SignedData<SharedSimKey>> SHARED_SIM_KEY_JSON = GsonAdapters.transform(BYTES_JSON,
			bytes -> SignedData.ofBytes(bytes, SharedSimKey::ofBytes),
			SignedData::toBytes);

	public static final TypeAdapter<RawCommit> COMMIT_JSON = GsonAdapters.transform(BYTES_JSON,
			RawCommit::ofBytes,
			RawCommit::toBytes);

	public static final TypeAdapter<CommitId> COMMIT_ID_JSON = GsonAdapters.transform(BYTES_JSON,
			CommitId::ofBytes,
			CommitId::toBytes);

	public static final TypeAdapter<Map<CommitId, RawCommit>> COMMIT_MAP_JSON = GsonAdapters.transform(GsonAdapters.ofMap(COMMIT_JSON),
			map -> map.entrySet().stream()
					.collect(toMap(
							entry -> urlDecodeCommitId(entry.getKey()),
							Map.Entry::getValue)),
			map -> map.entrySet().stream()
					.collect(toMap(
							entry -> urlEncodeCommitId(entry.getKey()),
							Map.Entry::getValue)));

	public static final class SaveTuple {
		public final Map<CommitId, RawCommit> commits;
		public final Set<SignedData<RawCommitHead>> heads;

		public SaveTuple(Map<CommitId, RawCommit> commits, Set<SignedData<RawCommitHead>> heads) {
			this.commits = commits;
			this.heads = heads;
		}

		public Map<CommitId, RawCommit> getCommits() {
			return commits;
		}

		public Set<SignedData<RawCommitHead>> getHeads() {
			return heads;
		}
	}

	public static final TypeAdapter<SaveTuple> SAVE_GSON = GsonAdapters.ofTuple(SaveTuple::new,
			"commits", SaveTuple::getCommits, COMMIT_MAP_JSON,
			"heads", SaveTuple::getHeads, GsonAdapters.ofSet(COMMIT_HEAD_JSON));

	public static final TypeAdapter<HeadsInfo> HEADS_INFO_GSON = GsonAdapters.ofTuple(HeadsInfo::new,
			"bases", HeadsInfo::getBases, GsonAdapters.ofSet(COMMIT_ID_JSON),
			"heads", HeadsInfo::getHeads, GsonAdapters.ofSet(COMMIT_ID_JSON));

	public static final TypeAdapter<HeadsDelta> HEADS_DELTA_GSON = GsonAdapters.ofTuple(HeadsDelta::new,
			"newHeads", HeadsDelta::getNewHeads, GsonAdapters.ofSet(COMMIT_HEAD_JSON),
			"excludedHeads", HeadsDelta::getExcludedHeads, GsonAdapters.ofSet(COMMIT_ID_JSON));

	public static String urlEncodeCommitId(CommitId commitId) {
		return Base64.getUrlEncoder().encodeToString(commitId.toBytes());
	}

	public static CommitId urlDecodeCommitId(String str) {
		return CommitId.ofBytes(Base64.getUrlDecoder().decode(str));
	}

	public static String urlEncodeRepositoryId(RepositoryName repositoryId) {
		return urlEncodePubKey(repositoryId.getPubKey()) + '/' + urlEncode(repositoryId.getRepositoryName(), "UTF-8");
	}

	public static RepositoryName urlDecodeRepositoryId(String str) {
		int pos = str.indexOf('/');
		PubKey pubKey = urlDecodePubKey(str.substring(0, pos));
		return new RepositoryName(pubKey, urlDecode(str.substring(pos + 1), "UTF-8"));
	}

	public static String urlEncodePubKey(PubKey pubKey) {
		ECPoint q = pubKey.getEcPublicKey().getQ();
		return "" +
				Base64.getUrlEncoder().encodeToString(q.getXCoord().toBigInteger().toByteArray()) + ':' +
				Base64.getUrlEncoder().encodeToString(q.getYCoord().toBigInteger().toByteArray());
	}

	public static PubKey urlDecodePubKey(String str) {
		int pos = str.indexOf(':');
		BigInteger x = new BigInteger(Base64.getUrlDecoder().decode(str.substring(0, pos)));
		BigInteger y = new BigInteger(Base64.getUrlDecoder().decode(str.substring(pos + 1)));
		return PubKey.ofQ(CryptoUtils.CURVE.getCurve().validatePoint(x, y));
	}

}
