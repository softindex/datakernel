#!/bin/bash

database_name=db_name
chunk_dir=/path/to/chunks
backup_dir=/path/to/backup

# execute 'backup.sql', which copies tables and prints current backup revision id on the last line
backup_revision_id=$(mysql -B -N ${database_name} < backup.sql | tail -1)

# if execution failed (duplicate key or could not get lock), exit with error
if [ "${backup_revision_id}" == "" ] || [ "${backup_revision_id}" == "-1" ]
then
	exit 1
fi

# create backup directory, using revision_id as a name
current_backup_dir="${backup_dir}/${backup_revision_id}"
mkdir ${current_backup_dir}

# iterate over chunk file names and copy
for chunk in $(mysql -B -N -e "SELECT CONCAT(id,'.log') FROM aggregation_db_chunk_backup WHERE backup_revision_id = ${backup_revision_id}" ${database_name})
do
	cp "${chunk_dir}/${chunk}" "${current_backup_dir}/${chunk}"
done