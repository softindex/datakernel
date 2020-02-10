package io.global.common.discovery;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.reflection.TypeT;
import io.datakernel.promise.Promise;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.AnnouncementStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import static io.datakernel.async.util.LogUtils.thisMethod;
import static io.datakernel.async.util.LogUtils.toLogger;
import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.common.Utils.loadResource;
import static io.datakernel.common.sql.SqlUtils.execute;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class MySqlAnnouncementStorage implements AnnouncementStorage {
	private static final Logger logger = LoggerFactory.getLogger(MySqlAnnouncementStorage.class);

	private static final StructuredCodec<SignedData<AnnounceData>> ANNOUNCEMENT_CODEC =
			REGISTRY.get(new TypeT<SignedData<AnnounceData>>() {});

	public static final String DEFAULT_ANNOUNCEMENTS_TABLE = "announcements";

	private final DataSource dataSource;

	private final Executor executor;

	private String tableAnnouncements = DEFAULT_ANNOUNCEMENTS_TABLE;

	private MySqlAnnouncementStorage(Executor executor, DataSource dataSource) {
		this.dataSource = dataSource;
		this.executor = executor;
	}

	public static MySqlAnnouncementStorage create(Executor executor, DataSource dataSource) {
		return new MySqlAnnouncementStorage(executor, dataSource);
	}

	public MySqlAnnouncementStorage withTableName(String tableAnnouncements) {
		this.tableAnnouncements = tableAnnouncements;
		return this;
	}

	@Override
	public Promise<Void> store(PubKey space, SignedData<AnnounceData> announceData) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement stmt = connection.prepareStatement(
								sql("INSERT INTO {announcements}(`pubKey`,`announcement`) VALUE (?, ?)" +
										" ON DUPLICATE KEY UPDATE `announcement`=?"))) {
							stmt.setString(1, space.asString());
							String announcementJson = toJson(ANNOUNCEMENT_CODEC, announceData);
							stmt.setString(2, announcementJson);
							stmt.setString(3, announcementJson);
							stmt.executeUpdate();
						}
					}
					return (Void) null;
				})
				.whenComplete(toLogger(logger, thisMethod(), space, announceData));
	}

	@Override
	public Promise<SignedData<AnnounceData>> load(PubKey space) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement stmt = connection.prepareStatement(
								sql("SELECT `announcement` FROM {announcements} " +
										"WHERE `pubKey`=?"))) {
							stmt.setString(1, space.asString());
							ResultSet resultSet = stmt.executeQuery();
							if (resultSet.next()) {
								return fromJson(ANNOUNCEMENT_CODEC, resultSet.getString(1));
							} else {
								return null;
							}
						}
					}
				})
				.whenComplete(toLogger(logger, thisMethod(), space));
	}

	@Override
	public Promise<Map<PubKey, SignedData<AnnounceData>>> loadAll() {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement stmt = connection.prepareStatement(
								sql("SELECT 'pubKey', `announcement` FROM {announcements} "))) {
							ResultSet resultSet = stmt.executeQuery();
							Map<PubKey, SignedData<AnnounceData>> result = new HashMap<>();
							while (resultSet.next()) {
								PubKey pubKey = PubKey.fromString(resultSet.getString(1));
								SignedData<AnnounceData> data = fromJson(ANNOUNCEMENT_CODEC, resultSet.getString(2));
								result.put(pubKey, data);
							}
							return result;
						}
					}
				})
				.whenComplete(toLogger(logger, thisMethod()));
	}

	public void initialize() throws IOException, SQLException {
		logger.trace("Initializing table");
		execute(dataSource, sql(new String(loadResource("sql/announcements.sql"), UTF_8)));
	}

	public void truncateTables() throws SQLException {
		logger.trace("Truncate tables");
		try (Connection connection = dataSource.getConnection()) {
			Statement statement = connection.createStatement();
			statement.execute(sql("TRUNCATE TABLE {announcements}"));
		}
	}

	private String sql(String sql) {
		return sql.replace("{announcements}", tableAnnouncements);
	}

}
