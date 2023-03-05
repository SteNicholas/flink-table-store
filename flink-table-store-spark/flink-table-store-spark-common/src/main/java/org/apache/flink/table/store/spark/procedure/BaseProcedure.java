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

package org.apache.flink.table.store.spark.procedure;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.procedure.Procedure;
import org.apache.spark.sql.connector.procedure.ProcedureBuilder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/** Base. */
public abstract class BaseProcedure implements Procedure {

    protected static final DataType STRING_MAP =
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);

    private final SparkSession spark;
    private final TableCatalog tableCatalog;

    protected BaseProcedure(TableCatalog tableCatalog) {
        this.spark = SparkSession.active();
        this.tableCatalog = tableCatalog;
    }

    protected SparkSession spark() {
        return this.spark;
    }

    protected TableCatalog tableCatalog() {
        return this.tableCatalog;
    }

    /**
     * Builder a.
     *
     * @param <T> type.
     */
    protected abstract static class Builder<T extends BaseProcedure> implements ProcedureBuilder {

        private TableCatalog tableCatalog;

        @Override
        public Builder<T> withTableCatalog(TableCatalog newTableCatalog) {
            this.tableCatalog = newTableCatalog;
            return this;
        }

        @Override
        public T build() {
            return doBuild();
        }

        protected abstract T doBuild();

        TableCatalog tableCatalog() {
            return tableCatalog;
        }
    }
}
