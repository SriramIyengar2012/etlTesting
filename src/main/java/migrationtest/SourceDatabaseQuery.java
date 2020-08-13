package migrationtest;

import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.coders.DoubleCoder;


import java.sql.ResultSet;
import java.util.List;

import static utils.Utils.*;



public class SourceDatabaseQuery {


public static void compareFieldCount(String query) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(JdbcIO.<KV<Integer, String>>read()
            .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
            .withQuery(query)
            .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()))
            .withRowMapper(new JdbcIO.RowMapper<KV<Integer, String>>() {
                public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
                    setCount(Integer.parseInt(resultSet.getString(1)));
                    return KV.of(resultSet.getInt(1), resultSet.getString(1));
                }
            })
    );
    pipeline.run().waitUntilFinish();


}

    public static void concatenateStringFields(String query) {


        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(JdbcIO.<KV<String, String>>read()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query)
                .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                .withRowMapper((JdbcIO.RowMapper<KV<String, String>>) resultSet -> KV.of("1", resultSet.getString(1)+ " " +resultSet.getString(2)))
        )
                .apply(ParDo.of(
                        new DoFn<KV<String, String>,String>() {
                            @ProcessElement
                            public void processLink(ProcessContext c){
                                System.out.println(c.element().getValue().toString());
                            }
                        }

                ));
        pipeline.run().waitUntilFinish();
    }

    public static void sumOfColumns(String query) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(JdbcIO.<KV<String, Double>>read()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query)
                .withCoder(KvCoder.of(StringUtf8Coder.of(),DoubleCoder.of()))
                .withRowMapper((JdbcIO.RowMapper<KV<String, Double>>) resultSet -> KV.of("1", resultSet.getDouble(1)))
        )

                .apply(Values.<Double>create())
                .apply(Sum.<Double>doublesGlobally())
                .apply(ParDo.of(new DoFn<Double, Double>() {
                    @ProcessElement
                    public void processElement(@Element Double word, OutputReceiver<Double> out) {
                        System.out.println(word);
                    }
                })
        );
        pipeline.run().waitUntilFinish();
    }


    public static void sumOfColumnsAndGroup(String query) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(JdbcIO.<KV<String, Double>>read()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query)
                .withCoder(KvCoder.of(StringUtf8Coder.of(),DoubleCoder.of()))
                .withRowMapper((JdbcIO.RowMapper<KV<String, Double>>) resultSet -> KV.of(resultSet.getString(1),resultSet.getDouble(2)))

        )
                .apply(GroupByKey.<String, Double>create())
                .apply(ParDo.of(new DoFn<KV<String, Iterable<Double>>, KV<String, Double>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        Iterable<Double> flows = c.element().getValue();
                        Double sum = 0.00;
                        Long numberOfRecords = 0L;
                        for (Double value : flows) {
                           sum+=value;
                            numberOfRecords++;
                        }
                        System.out.println(KV.of(c.element().getKey(),sum));
                    }
                }));
        pipeline.run().waitUntilFinish();
    }

    public static void multiColumns(String query, String columns) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        PCollection<Row> rows = pipeline.apply(JdbcIO.readRows()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query));
        PCollection<Row> output = rows.apply(Select.fieldNames(columns));
        output.apply(ParDo.of(new DoFn<Row, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
                List<Schema.Field> fld = c.element().getSchema().getFields();
                for (Schema.Field f : fld) {
                    if(f.getType().getTypeName().toString().equalsIgnoreCase("Double")) {
                        System.out.println(c.element().getDouble(f.getName()));
                        addValues(String.valueOf(c.element().getDouble(f.getName())));
                    }
                    if(f.getType().getTypeName().toString().equalsIgnoreCase("String")) {
                        System.out.println(c.element().getString(f.getName()));
                        addValues(String.valueOf(c.element().getString(f.getName())));
                    }
                    if(f.getType().getTypeName().toString().equalsIgnoreCase("int")) {
                        System.out.println(c.element().getInt16(f.getName()));
                        addValues(String.valueOf(c.element().getInt16(f.getName())));
                    }
                }

            }

                }

        ));
        pipeline.run().waitUntilFinish();

    }


    public static void schemaVerifyMultiColumns(String query, String columns) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        PCollection<Row> rows = pipeline.apply(JdbcIO.readRows()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query));
        PCollection<Row> output = rows.apply(Select.fieldNames(columns));
        output.apply(ParDo.of(new DoFn<Row, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
                List<Schema.Field> fld = c.element().getSchema().getFields();
                for (Schema.Field f : fld) {
                    addSchemaNames(f.getType().getTypeName().toString());
                }

            }

        }
        ));
        pipeline.run().waitUntilFinish();

    }


    }
