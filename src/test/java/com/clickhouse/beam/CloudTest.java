package com.clickhouse.beam;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.test.proto.SimpleProto;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoByteUtils;
import org.apache.beam.sdk.io.clickhouse.ClickHouseIO;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.AssertJUnit.*;

public class CloudTest {

    private static String database;
    private static String hostname;
    private static String password;
    private static String username;
    private static Client client = null;
    private static final int port = 8443;
    private static final int numberOfRecords = 1000;

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
    private static void dropTable(String tableName) throws ExecutionException, InterruptedException, TimeoutException {
        client.execute("DROP TABLE IF EXISTS " + database + "." + tableName).get(10, TimeUnit.SECONDS);
    }
    private void createDatabase() throws ExecutionException, InterruptedException, TimeoutException {
        client.execute("CREATE DATABASE IF NOT EXISTS " + database).get(10, TimeUnit.SECONDS);
    }
    private static long countRows(String tableName) throws ExecutionException, InterruptedException, TimeoutException {
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

    @DefaultSchema(JavaFieldSchema.class)
    public static class SimplePOJO {
        public DateTime f0;
        public DateTime f1;
        public float f2;
        public double f3;
        public byte f4;
        public short f5;
        public int f6;
        public long f7;
        public String f8;
        public short f9;
        public int f10;
        public long f11;
        public long f12;
        public String f13;
        public String f14;
        public String f15;
        public byte[] f16;
        public byte[] f17;
        public boolean f18;
        public String f19;

        public SimplePOJO(DateTime f0, DateTime f1, float f2, double f3, byte f4, short f5, int f6, long f7, String f8, short f9, int f10, long f11, long f12, String f13, String f14, String f15, byte[] f16, byte[] f17, boolean f18, String f19) {
            this.f0 = f0;
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
            this.f4 = f4;
            this.f5 = f5;
            this.f6 = f6;
            this.f7 = f7;
            this.f8 = f8;
            this.f9 = f9;
            this.f10 = f10;
            this.f11 = f11;
            this.f12 = f12;
            this.f13 = f13;
            this.f14 = f14;
            this.f15 = f15;
            this.f16 = f16;
            this.f17 = f17;
            this.f18 = f18;
            this.f19 = f19;
        }
        public SimplePOJO() {}

    }

    private List<SimplePOJO> createSimplePOJOs(int size) {
        List<SimplePOJO> simplePOJOs = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            simplePOJOs.add(new SimplePOJO(new DateTime(2030, 10, 1, 0, 0, 0, DateTimeZone.UTC),
                    new DateTime(2030, 10, 9, 8, 7, 6, DateTimeZone.UTC),
                    2.2f,
                    3.3,
                    (byte) 4,
                    (short) 5,
                    6,
                    i,
                    "eight",
                    (short) 9,
                    10,
                    11L,
                    12L,
                    "abc",
                    "cde",
                    "qwe",
                    new byte[] {'a', 's', 'd'},
                    new byte[] {'z', 'x', 'c'},
                    true,
                    "pojo" + i));
        }
        return simplePOJOs;
    }
    private static List<SimpleProto.Simple> createSimpleProto(int size) {
        List<SimpleProto.Simple> simpleProtos = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            SimpleProto.Simple simple =
                    SimpleProto.Simple.newBuilder()
                            .setIntField(i)
                            .setLongField(1000L + i)
                            .setFloatField(2.2f)
                            .setDoubleField(3.3)
                            .setBooleanField(true)
                            .setStringField("proto" + i)
                            .build();
            simpleProtos.add(simple);
        }
        return simpleProtos;
    }

    @Test
    public void simplePOJOInsertTest() throws ExecutionException, InterruptedException, TimeoutException {
        String tableName = "test_simple_pojo";

        List<SimplePOJO> pojos = createSimplePOJOs(numberOfRecords);

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
        pipeline.apply(Create.of(pojos))
                .apply(ClickHouseIO.write(String.format("jdbc:clickhouse://%s:%d/%s?user=%s&password=%s&ssl=true&compress=0", hostname, port, database, username, password), tableName));

        pipeline.run().waitUntilFinish();

        assertEquals("number of rows inserted ", numberOfRecords, countRows(tableName));
        dropTable(tableName);
    }
    @Test
    public static void simpleProtoInsertTest() throws ExecutionException, InterruptedException, TimeoutException {
        String tableName = "test_simple_proto";

        List<SimpleProto.Simple> protos = createSimpleProto(numberOfRecords);

        String sql = String.format("CREATE TABLE %s.%s ("
                + "intField  Int32,"
                + "longField  Int64,"
                + "floatField  Float32,"
                + "doubleField  Float64,"
                + "booleanField Bool,"
                + "stringField String"
                + ") ENGINE=MergeTree() ORDER BY intField ", database, tableName);

        Schema SCHEMA = ProtoByteUtils.getBeamSchemaFromProto("src/test/resources/simple/simple_desc.bin", SimpleProto.Simple.getDescriptor().getFullName());

        SerializableFunction<byte[], Row> protoBytesToRowFunction =
                ProtoByteUtils.getProtoBytesToRowFunction("src/test/resources/simple/simple_desc.bin", SimpleProto.Simple.getDescriptor().getFullName());

        client.execute(sql);

        Pipeline pipeline = Pipeline.create();
        PCollection<SimpleProto.Simple> simplePCollection = pipeline.apply(Create.of(protos));
        simplePCollection.apply("Convert to byte[]", ParDo.of(new DoFn<SimpleProto.Simple, Row>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                SimpleProto.Simple simple = c.element();
                byte[] byteArray = simple.toByteArray();
                Row row = protoBytesToRowFunction.apply(byteArray);
                c.output(row);
            }
        })).setRowSchema(SCHEMA)
                .apply(ClickHouseIO.write(String.format("jdbc:clickhouse://%s:%d/%s?user=%s&password=%s&ssl=true&compress=0", hostname, port, database, username, password), tableName));


        pipeline.run().waitUntilFinish();
        assertEquals("number of rows inserted ", numberOfRecords, countRows(tableName));
        dropTable(tableName);

    }
}
