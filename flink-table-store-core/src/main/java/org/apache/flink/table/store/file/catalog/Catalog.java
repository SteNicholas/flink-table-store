/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.catalog;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.store.file.schema.SchemaChange;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;

import java.util.List;
import java.util.Optional;

/**
 * This interface is responsible for reading and writing metadata such as database/table from a
 * table store catalog.
 */
public interface Catalog extends AutoCloseable {

    String DEFAULT_DATABASE = "default";

    /**
     * Get lock factory from catalog. Lock is used to support multiple concurrent writes on the
     * object store.
     */
    Optional<CatalogLock.Factory> lockFactory();

    // ------ databases ------

    /**
     * Get the names of all databases in this catalog.
     *
     * @return a list of the names of all databases
     */
    List<String> listDatabases();

    /**
     * Check if a database exists in this catalog.
     *
     * @param databaseName Name of the database
     * @return true if the given database exists in the catalog false otherwise
     */
    boolean databaseExists(String databaseName);

    /**
     * Create a database.
     *
     * @param name Name of the database to be created
     * @param ignoreIfExists Flag to specify behavior when a database with the given name already
     *     exists: if set to false, throw a DatabaseAlreadyExistException, if set to true, do
     *     nothing.
     * @throws DatabaseAlreadyExistException if the given database already exists and ignoreIfExists
     *     is false
     */
    void createDatabase(String name, boolean ignoreIfExists) throws DatabaseAlreadyExistException;

    /**
     * Drop a database.
     *
     * @param name Name of the database to be dropped.
     * @param ignoreIfNotExists Flag to specify behavior when the database does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @param cascade Flag to specify behavior when the database contains table or function: if set
     *     to true, delete all tables and functions in the database and then delete the database, if
     *     set to false, throw an exception.
     * @throws DatabaseNotEmptyException if the given database is not empty and isRestrict is true
     */
    void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException;

    // ------ tables ------

    /**
     * Get names of all tables under this database. An empty list is returned if none exists.
     *
     * @return a list of the names of all tables in this database
     * @throws DatabaseNotExistException if the database does not exist
     */
    List<String> listTables(String databaseName) throws DatabaseNotExistException;

    /**
     * Return the table location path identified by the given {@link ObjectPath}.
     *
     * @param tablePath Path of the table
     * @return The requested table location
     */
    Path getTableLocation(ObjectPath tablePath);

    /**
     * Return a {@link TableSchema} identified by the given {@link ObjectPath}.
     *
     * @param tablePath Path of the table
     * @return The requested table schema
     * @throws TableNotExistException if the target does not exist
     */
    TableSchema getTableSchema(ObjectPath tablePath) throws TableNotExistException;

    /**
     * Return a {@link FileStoreTable} identified by the given {@link ObjectPath}.
     *
     * @param tablePath Path of the table
     * @return The requested table
     * @throws TableNotExistException if the target does not exist
     */
    default FileStoreTable getTable(ObjectPath tablePath) throws TableNotExistException {
        TableSchema tableSchema = getTableSchema(tablePath);
        return FileStoreTableFactory.create(getTableLocation(tablePath), tableSchema);
    }

    /**
     * Check if a table exists in this catalog.
     *
     * @param tablePath Path of the table
     * @return true if the given table exists in the catalog false otherwise
     */
    boolean tableExists(ObjectPath tablePath);

    /**
     * Drop a table.
     *
     * @param tablePath Path of the table to be dropped
     * @param ignoreIfNotExists Flag to specify behavior when the table does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table does not exist
     */
    void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException;

    /**
     * Create a new table.
     *
     * @param tablePath path of the table to be created
     * @param tableSchema the table definition
     * @param ignoreIfExists flag to specify behavior when a table already exists at the given path:
     *     if set to false, it throws a TableAlreadyExistException, if set to true, do nothing.
     * @throws TableAlreadyExistException if table already exists and ignoreIfExists is false
     * @throws DatabaseNotExistException if the database in tablePath doesn't exist
     */
    void createTable(ObjectPath tablePath, UpdateSchema tableSchema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException;

    /**
     * Modify an existing table from {@link SchemaChange}s.
     *
     * @param tablePath path of the table to be modified
     * @param changes the schema changes
     * @param ignoreIfNotExists flag to specify behavior when the table does not exist: if set to
     *     false, throw an exception, if set to true, do nothing.
     * @throws TableNotExistException if the table does not exist
     */
    void alterTable(ObjectPath tablePath, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException;

    // ------ partitions ------

    /**
     * Get partition string of all partitions of the table.
     *
     * @param tablePath path of the table
     * @return a list of partition string of the table
     * @throws TableNotExistException thrown if the table does not exist in the catalog
     */
    List<String> listPartitions(ObjectPath tablePath) throws TableNotExistException;

    /**
     * Get partition string of all partitions that is under the given partition string in the table.
     *
     * @param tablePath path of the table
     * @param partition the partition string to list
     * @return a list of partition string that is under the given partition string in the table
     * @throws TableNotExistException thrown if the table does not exist in the catalog
     */
    List<String> listPartitions(ObjectPath tablePath, String partition)
            throws TableNotExistException;

    /**
     * Get partition string of partitions by expression filters in the table.
     *
     * <p>NOTE: For FieldReferenceExpression, the field index is based on schema of this table
     * instead of partition columns only.
     *
     * <p>The passed in predicates have been translated in conjunctive form.
     *
     * <p>If catalog does not support this interface at present, throw an {@link
     * UnsupportedOperationException} directly. If the catalog does not have a valid filter, throw
     * the {@link UnsupportedOperationException} directly. Planner will fallback to get all
     * partitions and filter by itself.
     *
     * @param tablePath path of the table
     * @param filters filters to push down filter to catalog
     * @return a list of partition string that is under the given partition string in the table
     * @throws TableNotExistException thrown if the table does not exist in the catalog
     */
    List<String> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException;

    /**
     * Get a partition of the given table. The given partition string keys and values need to be
     * matched exactly for a result.
     *
     * @param tablePath path of the table
     * @param partition partition string of partition to get
     * @return the requested partition
     * @throws PartitionNotExistException thrown if the partition doesn't exist
     */
    String getPartition(ObjectPath tablePath, String partition) throws PartitionNotExistException;

    /**
     * Check whether a partition exists or not.
     *
     * @param tablePath path of the table
     * @param partition partition string of the partition to check
     */
    boolean partitionExists(ObjectPath tablePath, String partition);

    /**
     * Drop a partition.
     *
     * @param tablePath path of the table.
     * @param partition partition string of the partition to drop
     * @param ignoreIfNotExists flag to specify behavior if the database does not exist: if set to
     *     false, throw an exception, if set to true, nothing happens.
     * @throws PartitionNotExistException thrown if the target partition does not exist
     */
    void dropPartition(ObjectPath tablePath, String partition, boolean ignoreIfNotExists)
            throws PartitionNotExistException;

    /** Exception for trying to drop on a database that is not empty. */
    class DatabaseNotEmptyException extends Exception {
        private static final String MSG = "Database %s is not empty.";

        private final String database;

        public DatabaseNotEmptyException(String database, Throwable cause) {
            super(String.format(MSG, database), cause);
            this.database = database;
        }

        public DatabaseNotEmptyException(String database) {
            this(database, null);
        }

        public String database() {
            return database;
        }
    }

    /** Exception for trying to create a database that already exists. */
    class DatabaseAlreadyExistException extends Exception {
        private static final String MSG = "Database %s already exists.";

        private final String database;

        public DatabaseAlreadyExistException(String database, Throwable cause) {
            super(String.format(MSG, database), cause);
            this.database = database;
        }

        public DatabaseAlreadyExistException(String database) {
            this(database, null);
        }

        public String database() {
            return database;
        }
    }

    /** Exception for trying to operate on a database that doesn't exist. */
    class DatabaseNotExistException extends Exception {
        private static final String MSG = "Database %s does not exist.";

        private final String database;

        public DatabaseNotExistException(String database, Throwable cause) {
            super(String.format(MSG, database), cause);
            this.database = database;
        }

        public DatabaseNotExistException(String database) {
            this(database, null);
        }

        public String database() {
            return database;
        }
    }

    /** Exception for trying to create a table that already exists. */
    class TableAlreadyExistException extends Exception {

        private static final String MSG = "Table %s already exists.";

        private final ObjectPath tablePath;

        public TableAlreadyExistException(ObjectPath tablePath) {
            this(tablePath, null);
        }

        public TableAlreadyExistException(ObjectPath tablePath, Throwable cause) {
            super(String.format(MSG, tablePath.getFullName()), cause);
            this.tablePath = tablePath;
        }

        public ObjectPath tablePath() {
            return tablePath;
        }
    }

    /** Exception for trying to operate on a table that doesn't exist. */
    class TableNotExistException extends Exception {

        private static final String MSG = "Table %s does not exist.";

        private final ObjectPath tablePath;

        public TableNotExistException(ObjectPath tablePath) {
            this(tablePath, null);
        }

        public TableNotExistException(ObjectPath tablePath, Throwable cause) {
            super(String.format(MSG, tablePath.getFullName()), cause);
            this.tablePath = tablePath;
        }

        public ObjectPath tablePath() {
            return tablePath;
        }
    }

    /**
     * Exception for operation on a partition that doesn't exist. The cause includes non-existent
     * table, non-partitioned table, invalid partition spec, etc.
     */
    class PartitionNotExistException extends Exception {

        private static final String MSG = "Partition %s of table %s does not exist.";

        private final ObjectPath tablePath;

        public PartitionNotExistException(ObjectPath tablePath, String partition) {
            this(tablePath, partition, null);
        }

        public PartitionNotExistException(ObjectPath tablePath, String partition, Throwable cause) {
            super(String.format(MSG, partition, tablePath.getFullName()), cause);
            this.tablePath = tablePath;
        }

        public ObjectPath getTablePath() {
            return tablePath;
        }
    }
}
