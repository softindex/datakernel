package io.global.common.discovery;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.util.TypeT;
import io.global.common.Hash;
import io.global.common.PubKey;
import io.global.common.SharedSimKey;
import io.global.common.SignedData;
import io.global.common.api.SharedKeyStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import static io.datakernel.codec.binary.BinaryUtils.decode;
import static io.datakernel.codec.binary.BinaryUtils.encodeAsArray;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static io.datakernel.util.SqlUtils.execute;
import static io.datakernel.util.Utils.loadResource;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class MySqlSharedKeyStorage implements SharedKeyStorage {
	private static final Logger logger = LoggerFactory.getLogger(MySqlSharedKeyStorage.class);

	private static final StructuredCodec<SignedData<SharedSimKey>> SHARED_SIM_KEY_CODEC =
			REGISTRY.get(new TypeT<SignedData<SharedSimKey>>() {});

	public static final String DEFAULT_RECEIVERS_TABLE = "receivers";
	public static final String DEFAULT_SHARED_KEYS_TABLE = "sharedKeys";

	private final DataSource dataSource;

	private Executor executor;

	private String tableReceivers = DEFAULT_RECEIVERS_TABLE;
	private String tableSharedKeys = DEFAULT_SHARED_KEYS_TABLE;

	private MySqlSharedKeyStorage(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public static MySqlSharedKeyStorage create(DataSource dataSource) {
		return new MySqlSharedKeyStorage(dataSource);
	}

	public MySqlSharedKeyStorage withExecutor(Executor executor) {
		this.executor = executor;
		return this;
	}

	public MySqlSharedKeyStorage withReceiversTableName(String tableReceivers) {
		this.tableReceivers = tableReceivers;
		return this;
	}

	public MySqlSharedKeyStorage withSharedKeysTableName(String tableSharedKeys) {
		this.tableSharedKeys = tableSharedKeys;
		return this;
	}

	@Override
	public Promise<Void> store(PubKey receiver, SignedData<SharedSimKey> signedSharedSimKey) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						connection.setAutoCommit(false);
						try (PreparedStatement stmt = connection.prepareStatement(
								sql("INSERT IGNORE INTO {receivers}(`receiver`) VALUES (?)"))) {
							stmt.setString(1, receiver.asString());
							stmt.executeUpdate();
						}

						try (PreparedStatement stmt = connection.prepareStatement(
								sql("INSERT IGNORE INTO {sharedKeys} VALUES " +
										"((SELECT id FROM {receivers} WHERE `receiver`=?), ?, ?)"))) {
							stmt.setString(1, receiver.asString());
							stmt.setString(2, signedSharedSimKey.getValue().getHash().asString());
							stmt.setBytes(3, encodeAsArray(SHARED_SIM_KEY_CODEC, signedSharedSimKey));
							stmt.executeUpdate();
						}

						connection.commit();
					}
					return (Void) null;
				})
				.acceptEx(toLogger(logger, thisMethod(), receiver, signedSharedSimKey));
	}

	@Override
	public Promise<SignedData<SharedSimKey>> load(PubKey receiver, Hash hash) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement stmt = connection.prepareStatement(
								sql("SELECT sharedKey FROM {sharedKeys} INNER JOIN {receivers} " +
										"ON `id`=`pk_id` " +
										"WHERE `receiver`=? AND `hash`=?"))) {
							stmt.setString(1, receiver.asString());
							stmt.setString(2, hash.asString());

							ResultSet resultSet = stmt.executeQuery();
							if (resultSet.next()) {
								return decode(SHARED_SIM_KEY_CODEC, resultSet.getBytes(1));
							} else {
								return null;
							}
						}
					}
				})
				.acceptEx(toLogger(logger, thisMethod(), receiver, hash));
	}

	@Override
	public Promise<List<SignedData<SharedSimKey>>> loadAll(PubKey receiver) {
		return Promise.ofBlockingCallable(executor,
				() -> {
					try (Connection connection = dataSource.getConnection()) {
						try (PreparedStatement stmt = connection.prepareStatement(
								sql("SELECT sharedKey FROM {sharedKeys} INNER JOIN {receivers} " +
										"ON `id`=`pk_id` " +
										"WHERE `pubKey`=?"))) {
							stmt.setString(1, receiver.asString());

							ResultSet resultSet = stmt.executeQuery();
							List<SignedData<SharedSimKey>> result = new ArrayList<>();
							while (resultSet.next()) {
								result.add(decode(SHARED_SIM_KEY_CODEC, resultSet.getBytes(1)));
							}
							return result;
						}
					}
				})
				.acceptEx(toLogger(logger, thisMethod(), receiver));
	}

	public void initialize() throws IOException, SQLException {
		logger.trace("Initializing table");
		execute(dataSource, sql(new String(loadResource("sql/sharedKeys.sql"), UTF_8)));
	}

	public void truncateTables() throws SQLException {
		logger.trace("Truncate tables");
		try (Connection connection = dataSource.getConnection()) {
			Statement statement = connection.createStatement();
			statement.execute(sql("TRUNCATE TABLE {receivers}"));
			statement.execute(sql("TRUNCATE TABLE {sharedKeys}"));
		}
	}

	private String sql(String sql) {
		return sql
				.replace("{receivers}", tableReceivers)
				.replace("{sharedKeys}", tableSharedKeys);
	}

}
