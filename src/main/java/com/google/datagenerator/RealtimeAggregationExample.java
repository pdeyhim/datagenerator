package com.google.datagenerator;

import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.json.JsonParser;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.datagenerator.utils.DurationUtils;
import model.Event;
import org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import com.google.gson.Gson;
import org.joda.time.Duration;

import java.awt.*;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 mvn compile exec:java \
-Dexec.mainClass=com.google.datagenerator.RealtimeAggregationExample \
-Dexec.args=" \
--project=deyhim-sandbox \
--stagingLocation=gs://deyhim-sandbox/dataflow/pipelines/realtimeagg/staging \
--tempLocation=gs://deyhim-sandbox/dataflow/pipelines/realtimeagg/temp \
--runner=DataflowRunner \
--inputTopic=projects/deyhim-sandbox/topics/deyhim-sandbox-to-pubsub \
--bigQueryTable=demo1.random_actors_agg_df"
 */

public class RealtimeAggregationExample {

    public interface Options extends PipelineOptions, StreamingOptions {
        @Description("The Cloud Pub/Sub topic to read from.")
        @Validation.Required
        ValueProvider<String> getInputTopic();
        void setInputTopic(ValueProvider<String> value);

        @Description("BigQuery table to write to")
        @Validation.Required
        ValueProvider<String> getBigQueryTable();
        void setBigQueryTable(ValueProvider<String> value);
    }

    private static final Logger LOG = LoggerFactory.getLogger(RealtimeAggregationExample.class);

    public static void main(String argv[]) {

        Options options = PipelineOptionsFactory.fromArgs(argv).withValidation().as(Options.class);
        run(options).run();
    }


    public static class EmitEvent extends DoFn<String, Event> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            String entry = c.element();
            Event event = new Gson().fromJson(entry,Event.class);
            c.output(event);
        }
    }


    public static class ExtractV1Field extends DoFn<Event,KV<String,Double>> {

        @DoFn.ProcessElement
        public void processElement(ProcessContext c) {
            Event event = c.element();
            String user_id = event.getUser_id();
            double v1 = event.getV1();
            c.output(KV.of(user_id, v1));
        }
    }


    public static class ConvertToTableRow extends DoFn<KV<String,Double>,TableRow> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c) {
            KV<String,Double> kv = c.element();

            DateTimeFormatter FOMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime localDateTime = LocalDateTime.now();
            String ldtString = FOMATTER.format(localDateTime);

            TableRow row = new TableRow()
                    .set("user_id",kv.getKey())
                    .set("v1_total",kv.getValue())
                    .set("ts",ldtString);
            c.output(row);
        }
    }

    public static TableSchema createTableSchema(String schema) {
        String[] fieldTypePairs = schema.split(",");
        List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();

        for(String entry : fieldTypePairs) {
            String[] fieldAndType = entry.split(":");
            fields.add(new TableFieldSchema().setName(fieldAndType[0]).setType(fieldAndType[1]));
        }

        return new TableSchema().setFields(fields);
    }


    public static Pipeline run(Options options) {
        Pipeline pipeline = Pipeline.create(options);

        PCollection<TableRow> pubSubMessages = pipeline
                .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
                .apply("TransformToEvent", ParDo.of(new EmitEvent()))
                .apply("GetV1",ParDo.of(new ExtractV1Field()))
                .apply("Window",Window.<KV<String,Double>>into( new GlobalWindows())
                        .triggering( Repeatedly.forever(
                                AfterProcessingTime.pastFirstElementInPane())
                        )
                        .accumulatingFiredPanes().withAllowedLateness(Duration.standardDays(1)))
                .apply("SUM",Sum.doublesPerKey())
                .apply("convertToTableRow",ParDo.of(new ConvertToTableRow()));

        pubSubMessages.apply("WriteToBQ",BigQueryIO.writeTableRows()
                .withSchema(createTableSchema("user_id:STRING,v1_total:FLOAT,ts:TIMESTAMP"))
                .to(options.getBigQueryTable())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        return pipeline;

    }

}
