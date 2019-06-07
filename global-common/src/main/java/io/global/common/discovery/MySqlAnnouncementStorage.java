package io.global.common.discovery;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.logger.LoggerFactory;
import io.datakernel.util.TypeT;
import io.global.common.PubKey;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.AnnouncementStorage;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.SqlUtils.execute;
import static io.datakernel.util.Utils.loadResource;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class MySqlAnnouncementStorage implements AnnouncementStorage {
	private static final Logger logger = LoggerFactory.getLogger(MySqlAnnouncementStorage.class.getName());

	private static final StructuredCodec<SignedData<AnnounceData>> ANNOUNCEMENT_CODEC =
			REGISTRY.get(new TypeT<SignedData<AnnounceData>>() {});

	public static final String DEFAULT_ANNOUNCEMENTS_TABLE = "announcements";

	private final DataSource dataSource;

	private Executor executor;

	private String tableAnnouncements = DEFAULT_ANNOUNCEMENTS_TABLE;

	private MySqlAnnouncementStorage(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public static MySqlAnnouncementStorage create(DataSource dataSource) {
		return new MySqlAnnouncementStorage(dataSource);
	}

	public MySqlAnnouncementStorage withExecutor(Executor executor) {
		this.executor = executor;
		return this;
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

	public void initialize() throws IOException, SQLException {
		logger.log(Level.FINE, "Initializing table");
		execute(dataSource, sql(new String(loadResource("sql/announcements.sql"), UTF_8)));
	}

	public void truncateTables() throws SQLException {
		logger.log(Level.FINE, "Truncate tables");
		try (Connection connection = dataSource.getConnection()) {
			Statement statement = connection.createStatement();
			statement.execute(sql("TRUNCATE TABLE {announcements}"));
		}
	}

	private String sql(String sql) {
		return sql.replace("{announcements}", tableAnnouncements);
	}

}
