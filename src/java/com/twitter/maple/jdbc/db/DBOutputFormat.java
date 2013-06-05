/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */
/**
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

package com.twitter.maple.jdbc.db;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

import cascading.tuple.TupleEntry;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A OutputFormat that sends the reduce output to a SQL table. <p/> {@link DBOutputFormat} accepts
 * &lt;key,value&gt; pairs, where key has a type extending DBWritable. Returned {@link RecordWriter}
 * writes <b>only the key</b> to the database with a batch SQL query.
 */
public class DBOutputFormat<K extends TupleEntry, V extends TupleEntry> implements OutputFormat<K, V> {
    private static final Log LOG = LogFactory.getLog(DBOutputFormat.class);

    /** A RecordWriter that writes the reduce output to a SQL table */
    protected class DBRecordWriter implements RecordWriter<K, V> {
        private Connection connection;
        private PreparedStatement insertStatement;
        private PreparedStatement updateStatement1;
        private PreparedStatement updateStatement2;
        private final int statementsBeforeExecute;

        private long statementsAdded = 0;
        private long insertStatementsCurrent = 0;
        private long updateStatementsCurrent = 0;
        private String[] fieldNames;
        private String[] updateNames;

        private DBRecordWriter(Connection connection, String tableName, 
                String[] fieldNames, String[] updateNames, int statementsBeforeExecute) throws IOException {

            String sqlInsert = constructInsertQuery(tableName, fieldNames);
            PreparedStatement insertPreparedStatement;

            try {
                insertPreparedStatement = connection.prepareStatement(sqlInsert);
                insertPreparedStatement.setEscapeProcessing(true); // should be on by default
            } catch (SQLException exception) {
                throw new IOException("unable to create statement for: " + sqlInsert, exception);
            }

            String sqlUpdate1 =
                updateNames != null ? constructUpdateQuery1(tableName, fieldNames, updateNames) : null;
            PreparedStatement updatePreparedStatement1 = null;

            try {
                updatePreparedStatement1 =
                    sqlUpdate1 != null ? connection.prepareStatement(sqlUpdate1) : null;
            } catch (SQLException exception) {
                throw new IOException("unable to create statement for: " + sqlUpdate1, exception);
            }

            String sqlUpdate2 =
                updateNames != null ? constructUpdateQuery2(tableName, fieldNames, updateNames) : null;
            PreparedStatement updatePreparedStatement2 = null;

            try {
                updatePreparedStatement2 =
                    sqlUpdate2 != null ? connection.prepareStatement(sqlUpdate2) : null;
            } catch (SQLException exception) {
                throw new IOException("unable to create statement for: " + sqlUpdate2, exception);
            }

            this.connection = connection;
            this.fieldNames = fieldNames;
            this.updateNames = updateNames;
            this.insertStatement = insertPreparedStatement;
            this.updateStatement1 = updatePreparedStatement1;
            this.updateStatement2 = updatePreparedStatement2;
            this.statementsBeforeExecute = statementsBeforeExecute;
        }

        /** {@inheritDoc} */
        public void close(Reporter reporter) throws IOException {
            executeBatch();

            try {
                if (insertStatement != null) { insertStatement.close(); }
                if (updateStatement1 != null) { updateStatement1.close(); }
                if (updateStatement2 != null) { updateStatement2.close(); }

                connection.commit();
            } catch (SQLException exception) {
                rollBack();

                createThrowMessage("unable to commit batch", 0, exception);
            } finally {
                try {
                    connection.close();
                } catch (SQLException exception) {
                    throw new IOException("unable to close connection", exception);
                }
            }
        }

        private void executeBatch() throws IOException {
            try {
                if (insertStatementsCurrent != 0) {
                    LOG.info(
                        "executing insert batch " + createBatchMessage(insertStatementsCurrent));

                    insertStatement.executeBatch();
                }

                insertStatementsCurrent = 0;
            } catch (SQLException exception) {
                rollBack();

                createThrowMessage("unable to execute insert batch", insertStatementsCurrent, exception);
            }

            try {
                if (updateStatementsCurrent != 0) {
                    LOG.info(
                        "executing update batch " + createBatchMessage(updateStatementsCurrent));

                    int[] result1 = updateStatement1.executeBatch();
                    int[] result2 = updateStatement2.executeBatch();

                    int count1 = 0;
                    int count2 = 0;

                    for (int value : result1) { count1 += value; }
                    for (int value : result2) { count2 += value; }

                    if ((count1 + count2) != updateStatementsCurrent) {
                        throw new IOException(
                            "update did not update same number of statements executed in batch, batch: "
                            + updateStatementsCurrent + "total: " + (count1 + count2)
                            + " updated: " + count1 + " inserted: " + count2);
                    }
                }

                updateStatementsCurrent = 0;
            } catch (SQLException exception) {
                rollBack();

                createThrowMessage("unable to execute update batch", updateStatementsCurrent, exception);
            }
        }

        private void rollBack() {
            try {
                connection.rollback();
            } catch (SQLException sqlException) {
                LOG.warn(StringUtils.stringifyException(sqlException));
            }
        }

        private String createBatchMessage(long currentStatements) {
            return String
                .format("[totstmts: %d][crntstmts: %d][batch: %d]", statementsAdded, currentStatements, statementsBeforeExecute);
        }

        private void createThrowMessage(String stateMessage, long currentStatements,
            SQLException exception) throws IOException {
            String message = exception.getMessage();

            message = message.substring(0, Math.min(500, message.length()));

            int messageLength = exception.getMessage().length();
            String batchMessage = createBatchMessage(currentStatements);
            String template = "%s [msglength: %d]%s %s";
            String errorMessage =
                String.format(template, stateMessage, messageLength, batchMessage, message);

            LOG.error(errorMessage, exception.getNextException());

            throw new IOException(errorMessage, exception.getNextException());
        }

        /** {@inheritDoc} */
        public synchronized void write(K key, V value) throws IOException {
            try {
                if (key == null) {
                    fillInsertStatement(value);
                    insertStatementsCurrent++;
                } else {
                    fillUpdateStatements(key, value);
                    updateStatementsCurrent++;
                }
            } catch (SQLException exception) {
                throw new IOException("unable to add batch statement", exception);
            }

            statementsAdded++;

            if (statementsAdded % statementsBeforeExecute == 0) { executeBatch(); }
        }

        private void fillInsertStatement(V value) throws SQLException {
            for( int i = 0; i < value.size(); i++ )
                insertStatement.setObject( i + 1, value.getObject(
                        "?" + this.fieldNames[i]));

            insertStatement.addBatch();
        }

        private void fillUpdateStatements(K key, V value) throws SQLException {
            int offset = 0;

            // all data first
            for( int i = 0; i < value.size(); i++ )
                updateStatement1.setObject( i + offset + 1, value.getObject(
                        "?" + this.fieldNames[i]));
            offset += value.size();

            // then all updatefields
            for( int i = 0; i < key.size(); i++ )
                updateStatement1.setObject( i + offset + 1, key.getObject(
                        "?" + this.updateNames[i]));
            offset += key.size();

            updateStatement1.addBatch(); // add the update
            offset = 0;

            // all data first
            for( int i = 0; i < value.size(); i++ )
                updateStatement2.setObject( i + offset + 1, value.getObject(
                        "?" + this.fieldNames[i]));
            offset += value.size();

            // then all updatefields
            for( int i = 0; i < key.size(); i++ )
                updateStatement2.setObject( i + offset + 1, key.getObject(
                        "?" + this.updateNames[i]));
            offset += key.size();

            updateStatement2.addBatch(); // add the insert
        }
    }

    /**
     * Constructs the query used as the prepared statement to insert data.
     *
     * @param table      the table to insert into
     * @param fieldNames the fields to insert into. If field names are unknown, supply an array of
     *                   nulls.
     */
    protected String constructInsertQuery(String table, String[] fieldNames) {
        if (fieldNames == null) {
            throw new IllegalArgumentException("Field names may not be null");
        }

        StringBuilder query = new StringBuilder();

        query.append("INSERT INTO ").append(table);

        if (fieldNames.length > 0 && fieldNames[0] != null) {
            query.append(" (");

            for (int i = 0; i < fieldNames.length; i++) {
                query.append(fieldNames[i]);

                if (i != fieldNames.length - 1) { query.append(","); }
            }

            query.append(")");

        }

        query.append(" VALUES (");

        for (int i = 0; i < fieldNames.length; i++) {
            query.append("?");

            if (i != fieldNames.length - 1) { query.append(","); }
        }

        query.append(");");

        return query.toString();
    }

    protected String constructUpdateQuery1(String table, String[] fieldNames, String[] updateNames) {
        if (fieldNames == null) {
            throw new IllegalArgumentException("field names may not be null");
        }

        StringBuilder query = new StringBuilder();

        query.append("UPDATE ").append(table);

        query.append(" SET ");

        if (fieldNames.length > 0 && fieldNames[0] != null) {
            int count = 0;

            for (int i = 0; i < fieldNames.length; i++) {
                if (count != 0) { query.append(","); }

                query.append(fieldNames[i]);
                query.append(" = ?");

                count++;
            }
        }

        query.append(" WHERE ");

        if (updateNames.length > 0 && updateNames[0] != null) {
            for (int i = 0; i < updateNames.length; i++) {
                query.append(updateNames[i]);
                query.append(" = ?");

                if (i != updateNames.length - 1) { query.append(" and "); }
            }
        }

        return query.toString();
    }

    protected String constructUpdateQuery2(String table, String[] fieldNames, String[] updateNames) {
        if (fieldNames == null) {
            throw new IllegalArgumentException("field names may not be null");
        }

        StringBuilder query = new StringBuilder();

        query.append("INSERT INTO ").append(table);

        if (fieldNames.length > 0 && fieldNames[0] != null) {
            query.append(" (");

            for (int i = 0; i < fieldNames.length; i++) {
                query.append(fieldNames[i]);

                if (i != fieldNames.length - 1) { query.append(","); }
            }
            query.append(")");
        }

        query.append(" SELECT ");

        for (int i = 0; i < fieldNames.length; i++) {
            query.append("?");

            if (i != fieldNames.length - 1) { query.append(","); }
        }
        
        query.append(" WHERE NOT EXISTS (SELECT 1 FROM ");
        query.append(table);
        query.append(" WHERE ");

        if (updateNames.length > 0 && updateNames[0] != null) {
            for (int i = 0; i < updateNames.length; i++) {
                query.append(updateNames[i]);
                query.append(" = ?");

                if (i != updateNames.length - 1) { query.append(" and "); }
            }
        }
        query.append(")");
        return query.toString();
    }

    /** {@inheritDoc} */
    public void checkOutputSpecs(FileSystem filesystem, JobConf job) throws IOException {
    }

    /** {@inheritDoc} */
    public RecordWriter<K, V> getRecordWriter(FileSystem filesystem, JobConf job, String name,
        Progressable progress) throws IOException {
        DBConfiguration dbConf = new DBConfiguration(job);

        String tableName = dbConf.getOutputTableName();
        String[] fieldNames = dbConf.getOutputFieldNames();
        String[] updateNames = dbConf.getOutputUpdateFieldNames();
        int batchStatements = dbConf.getBatchStatementsNum();

        Connection connection = dbConf.getConnection();

        configureConnection(connection);
        return new DBRecordWriter(connection, tableName, fieldNames, updateNames, batchStatements);
    }

    protected void configureConnection(Connection connection) {
        setAutoCommit(connection);
    }

    protected void setAutoCommit(Connection connection) {
        try {
            connection.setAutoCommit(false);
        } catch (Exception exception) {
            throw new RuntimeException("unable to set auto commit", exception);
        }
    }

    /**
     * Initializes the reduce-part of the job with the appropriate output settings
     *
     * @param job                 The job
     * @param dbOutputFormatClass
     * @param tableName           The table to insert data into
     * @param fieldNames          The field names in the table. If unknown, supply the appropriate
     */
    public static void setOutput(JobConf job, Class<? extends DBOutputFormat> dbOutputFormatClass,
        String tableName, String[] fieldNames, String[] updateFields, int batchSize) {
        if (dbOutputFormatClass == null) { job.setOutputFormat(DBOutputFormat.class); } else {
            job.setOutputFormat(dbOutputFormatClass);
        }

        // writing doesn't always happen in reduce
        job.setReduceSpeculativeExecution(false);
        job.setMapSpeculativeExecution(false);

        DBConfiguration dbConf = new DBConfiguration(job);

        dbConf.setOutputTableName(tableName);
        dbConf.setOutputFieldNames(fieldNames);

        if (updateFields != null) { dbConf.setOutputUpdateFieldNames(updateFields); }

        if (batchSize != -1) { dbConf.setBatchStatementsNum(batchSize); }
    }
}
