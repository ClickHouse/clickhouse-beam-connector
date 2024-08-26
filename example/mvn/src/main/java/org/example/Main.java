package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.clickhouse.ClickHouseIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.example.pojo.SimplePojo;
import org.joda.time.DateTime;

import java.io.IOException;

public class Main {
    public static void pojoExample(String host, String user, String password) {

//        CREATE TABLE simple_pojo (
//                byteField Int8,
//                shortField Int16,
//                intField Int32,
//                longField Int64,
//                floatField Float32,
//                doubleField Float64,
//                booleanField Bool,
//                stringField String
//        ) ENGINE = MergeTree()
//        ORDER BY byteField

        Pipeline pipeline = Pipeline.create();

        SimplePojo sp1 = new SimplePojo();
        SimplePojo sp2 = new SimplePojo();
        SimplePojo sp3 = new SimplePojo();

        pipeline.apply(Create.of(sp1, sp2, sp3))
                .apply(ClickHouseIO.write(String.format("jdbc:clickhouse://%s:8443/default?user=%s&password=%s&ssl=true", host, user, password), "simple_pojo"));
        pipeline.run().waitUntilFinish();
    }

    public static void csvExample(String host, String user, String password) {

//        CREATE TABLE csv_table (
//                name String,
//                `age` Int16,
//                verified Bool,
//                insertion_time Date,
//                ) ENGINE = MergeTree()
//        ORDER BY name

        Pipeline pipeline = Pipeline.create();

        Schema SCHEMA =
        Schema.builder()
                .addField(Schema.Field.of("name", Schema.FieldType.STRING).withNullable(true))
                .addField(Schema.Field.of("age", Schema.FieldType.INT16).withNullable(true))
                .addField(Schema.Field.of("verified", Schema.FieldType.BOOLEAN).withNullable(true))
                .addField(Schema.Field.of("insertion_time", Schema.FieldType.DATETIME).withNullable(false))
                .build();

        // Apply transforms to the pipeline.
        PCollection<String> helloWorldLines = pipeline.apply("ReadLines", TextIO.read().from("src/main/resources/input.csv"));
        PCollection<Row> rows = helloWorldLines.apply("ConvertToRow", ParDo.of(new DoFn<String, Row>() {
            @ProcessElement
            public void processElement(@Element String line, OutputReceiver<Row> out) {
                String[] values = line.split(",");
                System.out.println(values[0]);
                Row row = Row.withSchema(SCHEMA)
                        .addValues(values[0], Short.parseShort(values[1]), true, DateTime.now())
                        .build();
                out.output(row);
            }
        })).setRowSchema(SCHEMA);

        rows.apply("Write to ClickHouse",
                ClickHouseIO.write(String.format("jdbc:clickhouse://%s:8443/default?user=%s&password=%s&ssl=true", host, user, password), "csv_table"));
        // Run the pipeline.
        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args) throws IOException {
        // Note: Replace the placeholders with your ClickHouse host, user, and password.
        // The example assumes that the ClickHouse server is running on port 8443. (if needed change the port and remove ssl=true)

        String password = "{password}";
        String user = "default";
        String host = "{host}";

        pojoExample(host, user, password);
        csvExample(host, user, password);
    }
}
