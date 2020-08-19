package migrationtest;

import org.apache.beam.sdk.coders.*;
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
import utils.Utils;


import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static utils.Utils.*;


public class SourceDatabaseQuery {


    private static ResultSet resultSet;
    public static void dataProfileTest(String query) {

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
                .withRowMapper((JdbcIO.RowMapper<KV<String, String>>) resultSet -> KV.of("1", resultSet.getString(1) + " " + resultSet.getString(2)))
        )
                .apply(ParDo.of(
                        new DoFn<KV<String, String>, String>() {
                            @ProcessElement
                            public void processLink(ProcessContext c) {
                                System.out.println(c.element().getValue().toString());
                            }
                        }

                ));
        pipeline.run().waitUntilFinish();
    }

    public static void sumOfColumnswithDoubles(String query) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(JdbcIO.<KV<String, Double>>read()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query)
                .withCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()))
                .withRowMapper((JdbcIO.RowMapper<KV<String, Double>>) resultSet -> KV.of("1", resultSet.getDouble(1)))
        )

                .apply(Values.<Double>create())
                .apply(Sum.<Double>doublesGlobally())
                .apply(ParDo.of(new DoFn<Double, Double>() {
                            @ProcessElement
                            public void processElement(@Element Double value, OutputReceiver<Double> out) {
                                Utils.setSumCol(value);
                            }
                        })
                );
        pipeline.run().waitUntilFinish();
    }

    public static void sumOfColumnswithLongs(String query) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(JdbcIO.<KV<String, Long>>read()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query)
                .withCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
                .withRowMapper((JdbcIO.RowMapper<KV<String, Long>>) resultSet -> KV.of("1", resultSet.getLong(1)))
        )

                .apply(Values.<Long>create())
                .apply(Sum.<Long>longsGlobally())
                .apply(ParDo.of(new DoFn<Long, Long>() {
                            @ProcessElement
                            public void processElement(@Element Long value, OutputReceiver<Long> out) {
                                System.out.println(Double.valueOf(value));
                            }
                        })
                );
        pipeline.run().waitUntilFinish();
    }

    public static void sumOfColumnswithInt(String query) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(JdbcIO.<KV<String, Integer>>read()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query)
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()))
                .withRowMapper((JdbcIO.RowMapper<KV<String, Integer>>) resultSet -> KV.of("1", resultSet.getInt(1)))
        )

                .apply(Values.<Integer>create())
                .apply(Sum.<Integer>integersGlobally())
                .apply(ParDo.of(new DoFn<Integer, Integer>() {
                            @ProcessElement
                            public void processElement(@Element Integer word, OutputReceiver<Integer> out) {
                                System.out.println(word);
                            }
                        })
                );
        pipeline.run().waitUntilFinish();
    }



    public static void sumOfColumnsAndGroupDouble(String query) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(JdbcIO.<KV<String, Double>>read()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query)
                .withCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()))
                .withRowMapper((JdbcIO.RowMapper<KV<String, Double>>) resultSet -> KV.of(resultSet.getString(1), resultSet.getDouble(2)))

        )
                .apply(GroupByKey.<String, Double>create())
                .apply(ParDo.of(new DoFn<KV<String, Iterable<Double>>, KV<String, Double>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        Iterable<Double> flows = c.element().getValue();
                        Double sum = 0.00;
                        Long numberOfRecords = 0L;
                        for (Double value : flows) {
                            sum += value;
                            numberOfRecords++;
                        }
                        System.out.println(KV.of(c.element().getKey(), sum));
                    }
                }));
        pipeline.run().waitUntilFinish();
    }



    public static void sumOfColumnsAndGroupLong(String query) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(JdbcIO.<KV<String, Long>>read()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query)
                .withCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
                .withRowMapper((JdbcIO.RowMapper<KV<String, Long>>) resultSet -> KV.of(resultSet.getString(1), resultSet.getLong(2)))

        )
                .apply(GroupByKey.<String, Long>create())
                .apply(ParDo.of(new DoFn<KV<String, Iterable<Long>>, KV<String, Long>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        Iterable<Long> flows = c.element().getValue();
                        Long sum = 0L;
                        Long numberOfRecords = 0L;
                        for (Long value : flows) {
                            sum += value;
                            numberOfRecords++;
                        }
                        System.out.println(KV.of(c.element().getKey(), sum));
                    }
                }));
        pipeline.run().waitUntilFinish();
    }



    public static void sumOfColumnsAndGroupInt(String query) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(JdbcIO.<KV<String, Integer>>read()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query)
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()))
                .withRowMapper((JdbcIO.RowMapper<KV<String, Integer>>) resultSet -> KV.of(resultSet.getString(1), resultSet.getInt(2)))

        )
                .apply(GroupByKey.<String, Integer>create())
                .apply(ParDo.of(new DoFn<KV<String, Iterable<Integer>>, KV<String, Long>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        Iterable<Integer> flows = c.element().getValue();
                        int sum = 0;
                        Long numberOfRecords = 0L;
                        for (int value : flows) {
                            sum += value;
                            numberOfRecords++;
                        }
                        System.out.println(KV.of(c.element().getKey(), sum));
                    }
                }));
        pipeline.run().waitUntilFinish();
    }

    public static void multiColumnsData(String query, String columns) {
        String [] columnList = columns.split(",");
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        PCollection<Row> rows = pipeline.apply(JdbcIO.readRows()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query));
        PCollection<Row> output = rows.apply(Select.fieldNames(columnList));
        output.apply(ParDo.of(new DoFn<Row, String>() {
                                  @ProcessElement
                                  public void processElement(ProcessContext c) throws Exception {
                                      List<Schema.Field> fld = c.element().getSchema().getFields();
                                      for (Schema.Field f : fld) {
                                          if (f.getType().getTypeName().toString().equalsIgnoreCase("Double")) {
                                              System.out.println(c.element().getDouble(f.getName()));
                                              addValues(String.valueOf(c.element().getDouble(f.getName())));
                                          }
                                          if (f.getType().getTypeName().toString().equalsIgnoreCase("String")) {
                                              System.out.println(c.element().getString(f.getName()));
                                              addValues(String.valueOf(c.element().getString(f.getName())));
                                          }
                                          if (f.getType().getTypeName().toString().equalsIgnoreCase("int16")) {
                                              System.out.println(c.element().getInt16(f.getName()));
                                              addValues(String.valueOf(c.element().getInt16(f.getName())));
                                          }
                                          if (f.getType().getTypeName().toString().equalsIgnoreCase("int32")) {
                                              System.out.println(c.element().getInt32(f.getName()));
                                              addValues(String.valueOf(c.element().getInt32(f.getName())));
                                          }
                                      }
                                  }
                              }

        ));
        pipeline.run().waitUntilFinish();

    }

    public static void multiColumnsSum(String query, String columns) {
        String [] columnList = columns.split(",");
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        PCollection<Row> rows = pipeline.apply(JdbcIO.readRows()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query));
        PCollection<Row> output = rows.apply(Select.fieldNames(columnList));
       PCollection <KV<String,String>> output2 =  output.apply(ParDo.of(new DoFn<Row, KV<String,String>>() {
                                  @ProcessElement
                                  public void processElement(ProcessContext c) throws Exception {
                                      List<Schema.Field> fld = c.element().getSchema().getFields();
                                      for (Schema.Field f : fld) {

                                          if (f.getType().getTypeName().toString().equalsIgnoreCase("Double")) {
                                             System.out.println(c.element().getDouble(f.getName()));
                                             c.output(KV.of(f.getName(), String.valueOf(c.element().getDouble(f.getName()))));

                                          }
                                          if (f.getType().getTypeName().toString().equalsIgnoreCase("String")) {
                                              System.out.println(c.element().getString(f.getName()));
                                              c.output(KV.of(f.getName(), String.valueOf(c.element().getString(f.getName()))));

                                          }
                                          if (f.getType().getTypeName().toString().equalsIgnoreCase("int16")) {
                                              System.out.println(c.element().getInt16(f.getName()));
                                              c.output(KV.of(f.getName(), String.valueOf(c.element().getInt16(f.getName()))));

                                          }
                                          if (f.getType().getTypeName().toString().equalsIgnoreCase("int32")) {
                                              System.out.println(c.element().getInt32(f.getName()));
                                              c.output(KV.of(f.getName(), String.valueOf(c.element().getInt32(f.getName()))));

                                          }

                                      }
                                  }
                              }

        ));
       output2.apply(GroupByKey.<String, String>create())
               .apply(ParDo.of(new DoFn<KV<String,Iterable<String>>, Double>() {
           @ProcessElement
           public void processElement(ProcessContext c) throws Exception {
               Iterable<String> flows = c.element().getValue();

               Double sum = 0.0;
               Long numberOfRecords = 0L;
               for (String value : flows) {
                   sum += Double.valueOf(value);
                   numberOfRecords++;
               }
               System.out.println(KV.of(c.element().getKey(), sum));
               Utils.getMultiColumnData().put(c.element().getKey(), String.valueOf(sum));


           }


               }

       ));
        pipeline.run().waitUntilFinish();

    }


    public static void schemaVerifyMultiColumns(String query, String columns) {
        String [] columnList = columns.split(",");
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        PCollection<Row> rows = pipeline.apply(JdbcIO.readRows()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query));
        PCollection<Row> output = rows.apply(Select.fieldNames(columnList));
        output.apply(ParDo.of(new DoFn<Row, KV<String,String>>() {
                                  @ProcessElement
                                  public void processElement(ProcessContext c) throws Exception {
                                      List<Schema.Field> fld = c.element().getSchema().getFields();
                                      for (Schema.Field f : fld)
                                          Utils.getSchemaNames().put(f.getName(), f.getType().getTypeName().toString());
                                  }
                              }

        ));
        pipeline.run().waitUntilFinish();

    }

    public static void checkNullConstraint(String query, String columns) {
        String [] columnList = columns.split(",");
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        PCollection<Row> rows = pipeline.apply(JdbcIO.readRows()
                .withDataSourceConfiguration(DatabaseConnection.sourceConnection())
                .withQuery(query));
        PCollection<Row> output = rows.apply(Select.fieldNames(columnList));
        output.apply(ParDo.of(new DoFn<Row, String>() {
                                  @ProcessElement
                                  public void processElement(ProcessContext c) throws Exception {
                                      List<Schema.Field> fld = c.element().getSchema().getFields();
                                      for (Schema.Field f : fld) {
                                          Utils.getNullValues().put(f.getName(),f.getType().getNullable().toString());
                                      }
                                  }

                              }

        ));
        pipeline.run().waitUntilFinish();

    }

    public static void getColumnSizeSource(String table){
        try {
            Statement stmt= DatabaseConnection.targetConnection().createStatement();
            resultSet = stmt.executeQuery("select * from"+table);
            resultSetToMapColumnSize(resultSet);
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }



}







