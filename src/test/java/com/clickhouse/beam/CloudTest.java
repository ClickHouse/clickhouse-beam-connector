package com.clickhouse.beam;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.Records;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.clickhouse.ClickHouseIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.AssertJUnit.*;

public class CloudTest {

    private String database;
    private String hostname;
    private String password;
    private String username;
    private Client client = null;
    private final int port = 8443;
    private final int numberOfRecords = 1000;

    @BeforeClass
    public void setUp() throws ExecutionException, InterruptedException, TimeoutException {
        // create database dynamically
        database = "beam_test_" + System.currentTimeMillis();
        hostname = System.getProperty("CLICKHOUSE_HOSTNAME");
        password = System.getProperty("CLICKHOUSE_PASSWORD");
        username = "default";
        client = new Client.Builder()
                .setPassword(password)
                .setUsername(username)
                .addEndpoint(Protocol.HTTP, hostname, port, true)
                .compressServerResponse(false)
                .compressClientRequest(false)
                .build();
        // ping to wake up the server
        client.ping();
        createDatabase();
    }
    private void dropTable(String tableName) throws ExecutionException, InterruptedException, TimeoutException {
        client.execute("DROP TABLE IF EXISTS " + database + "." + tableName).get(10, TimeUnit.SECONDS);
    }
    private void createDatabase() throws ExecutionException, InterruptedException, TimeoutException {
        client.execute("CREATE DATABASE IF NOT EXISTS " + database).get(10, TimeUnit.SECONDS);
    }
    private long countRows(String tableName) throws ExecutionException, InterruptedException, TimeoutException {
        String sql = "SELECT count(*) FROM " + database + "." + tableName;
        Records records = client.queryRecords(sql).get(10, TimeUnit.SECONDS);
        for (GenericRecord record : records) {
            return record.getBigInteger(1).longValue();
        }
        return -1;
    }

    private Row createRow(Schema schema, int index) {
        Row row1 =
                Row.withSchema(schema)
                        .addValue(new DateTime(2030, 10, 1, 0, 0, 0, DateTimeZone.UTC))
                        .addValue(new DateTime(2030, 10, 9, 8, 7, 6, DateTimeZone.UTC))
                        .addValue(2.2f)
                        .addValue(3.3)
                        .addValue((byte) 4)
                        .addValue((short) 5)
                        .addValue(6)
                        .addValue(7L)
                        .addValue("eight")
                        .addValue((short) 9)
                        .addValue(index)
                        .addValue(11L)
                        .addValue(12L)
                        .addValue("abc")
                        .addValue("cde")
                        .addValue("qwe")
                        .addValue(new byte[] {'a', 's', 'd'})
                        .addValue(new byte[] {'z', 'x', 'c'})
                        .addValue(true)
                        .addValue("lowcardenality")
                        .build();

        return row1;
    }
    private List<Row> createRows(Schema schema, int size) {
        List<Row> listOfRows = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            listOfRows.add(createRow(schema, i));
        }
        return listOfRows;
    }

    @Test
    public void simpleInsertTest() throws ExecutionException, InterruptedException, TimeoutException {
        String tableName = "test_primitive_types";
        Schema schema =
                Schema.of(
                        Schema.Field.of("f0", Schema.FieldType.DATETIME),
                        Schema.Field.of("f1", Schema.FieldType.DATETIME),
                        Schema.Field.of("f2", Schema.FieldType.FLOAT),
                        Schema.Field.of("f3", Schema.FieldType.DOUBLE),
                        Schema.Field.of("f4", Schema.FieldType.BYTE),
                        Schema.Field.of("f5", Schema.FieldType.INT16),
                        Schema.Field.of("f6", Schema.FieldType.INT32),
                        Schema.Field.of("f7", Schema.FieldType.INT64),
                        Schema.Field.of("f8", Schema.FieldType.STRING),
                        Schema.Field.of("f9", Schema.FieldType.INT16),
                        Schema.Field.of("f10", Schema.FieldType.INT32),
                        Schema.Field.of("f11", Schema.FieldType.INT64),
                        Schema.Field.of("f12", Schema.FieldType.INT64),
                        Schema.Field.of("f13", Schema.FieldType.STRING),
                        Schema.Field.of("f14", Schema.FieldType.STRING),
                        Schema.Field.of("f15", Schema.FieldType.STRING),
                        Schema.Field.of("f16", Schema.FieldType.BYTES),
                        Schema.Field.of("f17", Schema.FieldType.logicalType(FixedBytes.of(3))),
                        Schema.Field.of("f18", Schema.FieldType.BOOLEAN),
                        Schema.Field.of("f19", Schema.FieldType.STRING));

        List<Row> rows = createRows(schema, numberOfRecords);
        // We need to create table

        String sql = String.format("CREATE TABLE %s.%s ("
                + "f0  Date,"
                + "f1  DateTime,"
                + "f2  Float32,"
                + "f3  Float64,"
                + "f4  Int8,"
                + "f5  Int16,"
                + "f6  Int32,"
                + "f7  Int64,"
                + "f8  String,"
                + "f9  UInt8,"
                + "f10 UInt16,"
                + "f11 UInt32,"
                + "f12 UInt64,"
                + "f13 Enum8('abc' = 1, 'cde' = 2),"
                + "f14 Enum16('abc' = -1, 'cde' = -2),"
                + "f15 FixedString(3),"
                + "f16 FixedString(3),"
                + "f17 FixedString(3),"
                + "f18 Bool,"
                + "f19 LowCardinality(String)"
                + ") ENGINE=MergeTree() ORDER BY f4 ", database, tableName);

        client.execute(sql);

        Pipeline pipeline = Pipeline.create();
        pipeline.apply(Create.of(rows).withRowSchema(schema))
                 .apply(ClickHouseIO.write(String.format("jdbc:clickhouse://%s:%d/%s?user=%s&password=%s&ssl=true&compress=0", hostname, port, database, username, password), tableName));

        pipeline.run().waitUntilFinish();
        assertEquals("number of rows inserted ", numberOfRecords, countRows(tableName));
        dropTable(tableName);
    }

}
