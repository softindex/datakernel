DROP PROCEDURE IF EXISTS perform_backup;

DELIMITER //

CREATE PROCEDURE perform_backup()

    proc_label: BEGIN
    IF (SELECT get_lock('cube_lock', 180) != 1)
    THEN
      SELECT -1;
      LEAVE proc_label;
    END IF;

    SET @max_revision = (SELECT MAX(revision_id)
                         FROM aggregation_db_chunk);

    INSERT INTO aggregation_db_chunk_backup
      SELECT
        @max_revision AS backup_revision_id,
        aggregation_db_chunk.*
      FROM aggregation_db_chunk
      WHERE consolidated_revision_id IS NULL;

    INSERT INTO aggregation_db_log_backup
      SELECT
        @max_revision AS backup_revision_id,
        aggregation_db_log.*
      FROM aggregation_db_log;

    INSERT INTO aggregation_db_revision_backup
      SELECT
        @max_revision AS backup_revision_id,
        aggregation_db_revision.*
      FROM aggregation_db_revision;

    SELECT release_lock('cube_lock');

    SELECT @max_revision;
  END

//

DELIMITER ;

CALL perform_backup();

DROP PROCEDURE perform_backup;
