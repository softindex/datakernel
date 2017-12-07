/**
 * This class is generated by jOOQ
 */
package io.datakernel.aggregation.sql;

import io.datakernel.aggregation.sql.tables.AggregationDbChunk;
import io.datakernel.aggregation.sql.tables.AggregationDbRevision;
import io.datakernel.aggregation.sql.tables.records.AggregationDbChunkRecord;
import io.datakernel.aggregation.sql.tables.records.AggregationDbRevisionRecord;
import org.jooq.Identity;
import org.jooq.UniqueKey;
import org.jooq.impl.AbstractKeys;

import javax.annotation.Generated;


/**
 * A class modelling foreign key relationships between tables of the <code></code> 
 * schema
 */
@Generated(
	value = {
		"http://www.jooq.org",
		"jOOQ version:3.7.2"
	},
	comments = "This class is generated by jOOQ"
)
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Keys {

	// -------------------------------------------------------------------------
	// IDENTITY definitions
	// -------------------------------------------------------------------------

	public static final Identity<AggregationDbChunkRecord, Long> IDENTITY_AGGREGATION_DB_CHUNK = Identities0.IDENTITY_AGGREGATION_DB_CHUNK;
	public static final Identity<AggregationDbRevisionRecord, Integer> IDENTITY_AGGREGATION_DB_REVISION = Identities0.IDENTITY_AGGREGATION_DB_REVISION;

	// -------------------------------------------------------------------------
	// UNIQUE and PRIMARY KEY definitions
	// -------------------------------------------------------------------------

	public static final UniqueKey<AggregationDbChunkRecord> KEY_AGGREGATION_DB_CHUNK_PRIMARY = UniqueKeys0.KEY_AGGREGATION_DB_CHUNK_PRIMARY;
	public static final UniqueKey<AggregationDbRevisionRecord> KEY_AGGREGATION_DB_REVISION_PRIMARY = UniqueKeys0.KEY_AGGREGATION_DB_REVISION_PRIMARY;

	// -------------------------------------------------------------------------
	// FOREIGN KEY definitions
	// -------------------------------------------------------------------------


	// -------------------------------------------------------------------------
	// [#1459] distribute members to avoid static initialisers > 64kb
	// -------------------------------------------------------------------------

	private static class Identities0 extends AbstractKeys {
		public static Identity<AggregationDbChunkRecord, Long> IDENTITY_AGGREGATION_DB_CHUNK = createIdentity(AggregationDbChunk.AGGREGATION_DB_CHUNK, AggregationDbChunk.AGGREGATION_DB_CHUNK.ID);
		public static Identity<AggregationDbRevisionRecord, Integer> IDENTITY_AGGREGATION_DB_REVISION = createIdentity(AggregationDbRevision.AGGREGATION_DB_REVISION, AggregationDbRevision.AGGREGATION_DB_REVISION.ID);
	}

	private static class UniqueKeys0 extends AbstractKeys {
		public static final UniqueKey<AggregationDbChunkRecord> KEY_AGGREGATION_DB_CHUNK_PRIMARY = createUniqueKey(AggregationDbChunk.AGGREGATION_DB_CHUNK, AggregationDbChunk.AGGREGATION_DB_CHUNK.ID);
		public static final UniqueKey<AggregationDbRevisionRecord> KEY_AGGREGATION_DB_REVISION_PRIMARY = createUniqueKey(AggregationDbRevision.AGGREGATION_DB_REVISION, AggregationDbRevision.AGGREGATION_DB_REVISION.ID);
	}
}