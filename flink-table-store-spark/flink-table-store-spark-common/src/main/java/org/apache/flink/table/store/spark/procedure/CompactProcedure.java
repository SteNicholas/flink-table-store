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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.procedure.Procedure;
import org.apache.spark.sql.connector.procedure.ProcedureBuilder;
import org.apache.spark.sql.connector.procedure.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Compact {@link Procedure procedure} runs compaction on table. */
public class CompactProcedure extends BaseProcedure {

    private static final String NAME = "compact";

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("warehouse", DataTypes.StringType),
                ProcedureParameter.required("database", DataTypes.StringType),
                ProcedureParameter.required("table", DataTypes.StringType),
                ProcedureParameter.optional("partition", STRING_MAP)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField(
                                "added_files_count", DataTypes.LongType, false, Metadata.empty()),
                        new StructField(
                                "changed_partition_count",
                                DataTypes.LongType,
                                false,
                                Metadata.empty()),
                    });

    private CompactProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    public static String name() {
        return NAME;
    }

    public static ProcedureBuilder builder() {
        return new Builder<CompactProcedure>() {
            @Override
            protected CompactProcedure doBuild() {
                return new CompactProcedure(tableCatalog());
            }
        };
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        //        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        return null;
    }

    @Override
    public String description() {
        StringBuilder sb = new StringBuilder();
        sb.append("Procedure \"compact\" runs a dedicated job for compacting specified table.\n");
        sb.append("Syntax:\n");
        sb.append("\t")
                .append(
                        "call compact(warehouse => <warehouse-path>, database => <database-name>, table => <table-name> [partition => <partition-name>])\n");
        sb.append("Partition name syntax:\n");
        sb.append("\t").append("key1=value1,key2=value2,...\n");
        sb.append("Examples:\n");
        sb.append("\t")
                .append(
                        "call compact(warehouse => 'hdfs:///path/to/warehouse', database => 'test_db', table => 'test_table')\n");
        sb.append("\t")
                .append(
                        "call compact(warehouse => 'hdfs:///path/to/warehouse, database => 'test_db', table => 'test_table', partition => 'dt=20221126,hh=08')\n");
        return sb.toString();
    }
}
