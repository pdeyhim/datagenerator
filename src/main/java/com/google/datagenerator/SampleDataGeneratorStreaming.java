/*


-Dexec.mainClass=com.google.cloud.teleport.templates.PubSubToBigQuery \
-Dexec.args=" \
--project=deyhim-sandbox \
--stagingLocation=gs://deyhim-sandbox/dataflow/pipelines/gcstopubsub/staging \
--tempLocation=gs://deyhim-sandbox/dataflow/pipelines/gcstopubsub/temp2 \
--runner=DataflowRunner \
--inputTopic=projects/deyhim-sandbox/topics/deyhim-sandbox-to-pubsub \
--outputTableSpec=deyhim-sandbox:demo1.random_actors \
--templateLocation=gs://deyhim-sandbox/dataflow-templates/PubSubToBigQuery"


mvn compile exec:java \
 -Dexec.mainClass=com.google.cloud.teleport.examples.SampleDataGeneratorBatch \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=deyhim-sandbox \
 --stagingLocation=gs://deyhim-sandbox/dataflow/pipelines/sampler/staging \
 --tempLocation=gs://deyhim-sandbox/dataflow/pipelines/sampler/temp \
 --runner=DataflowRunner \
 --windowDuration=10m \
 --numShards=5 \
 --qps=10 \
 --schemaLocation=gs://deyhim-sandbox/sampler/sampleSchema.json \
 --outputDirectory=gs://deyhim-sandbox/dataflow-sampler/ \
 --outputFilenamePrefix=user-simulated-data- \
 --outputFilenameSuffix=.json"



 mvn compile exec:java \
 -Dexec.mainClass=com.google.cloud.teleport.examples.SampleDataGeneratorStreaming \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=deyhim-sandbox \
 --stagingLocation=gs://deyhim-sandbox/dataflow/pipelines/sampler/staging \
 --tempLocation=gs://deyhim-sandbox/dataflow/pipelines/sampler/temp \
 --runner=DataflowRunner \
 --windowDuration=5s \
 --numShards=5 \
 --qps=100 \
 --schemaLocation=gs://deyhim-sandbox/sampler/sampleSchema.json \
 --outputTopic=projects/deyhim-sandbox/topics/deyhim-sandbox-to-pubsub"

 mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.TextToPubsubStream \
-Dexec.args=" \
--project=deyhim-sandbox \
--stagingLocation=gs://deyhim-sandbox/dataflow/pipelines/gcstopubsub/staging \
--tempLocation=gs://deyhim-sandbox/dataflow/pipelines/gcstopubsub/temp2 \
--runner=DataflowRunner \
--inputFilePattern=gs://deyhim-sandbox/dataflow-sampler/user-simulated-data-* \
--outputTopic=projects/deyhim-sandbox/topics/deyhim-sandbox-to-pubsub"


 mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.PubSubToBigQuery \
-Dexec.args=" \
--project=deyhim-sandbox \
--stagingLocation=gs://deyhim-sandbox/dataflow/pipelines/gcstopubsub/staging \
--tempLocation=gs://deyhim-sandbox/dataflow/pipelines/gcstopubsub/temp2 \
--runner=DataflowRunner \
--inputTopic=projects/deyhim-sandbox/topics/deyhim-sandbox-to-pubsub \
--outputTableSpec=deyhim-sandbox:demo1.random_actors \
--numWorkers=3"

CREATE OR REPLACE TABLE `demo1.random_actors` (ts STRING, user_id INT64, actor STRING,location STRUCT<longitude STRING, latitude STRING, zip STRING>, v1 FLOAT64,v2 FLOAT64,v3 FLOAT64,v4 FLOAT64,v5 FLOAT64,v6 FLOAT64)
 */


/*

https://github.com/confluentinc/kafka-connect-datagen/tree/master/src/main/resources

 */

package com.google.datagenerator;

import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.datagenerator.utils.DurationUtils;
import com.mapr.synth.samplers.SchemaSampler;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

public class SampleDataGeneratorStreaming {

    public interface Options extends PipelineOptions, StreamingOptions {

        @Description("The QPS which the benchmark should output to Pub/Sub.")
        @Validation.Required
        Long getQps();
        void setQps(Long value);

        @Description("Schema location on GCS")
        @Validation.Required
        String getSchemaLocation();
        void setSchemaLocation(String value);

        @Description("The Cloud Pub/Sub topic to write to.")
        ValueProvider<String> getOutputTopic();
        void setOutputTopic(ValueProvider<String> value);

        @Description(
                "The shard template of the output file. Specified as repeating sequences "
                        + "of the letters 'S' or 'N' (example: SSS-NNN). These are replaced with the "
                        + "shard number, or number of shards respectively")
        @Default.String("W-P-SS-of-NN")
        ValueProvider<String> getOutputShardTemplate();

        void setOutputShardTemplate(ValueProvider<String> value);

        @Description("The maximum number of output shards produced when writing.")
        @Default.Integer(1)
        Integer getNumShards();

        void setNumShards(Integer value);

        @Description(
                "The window duration in which data will be written. Defaults to 5m. "
                        + "Allowed formats are: "
                        + "Ns (for seconds, example: 5s), "
                        + "Nm (for minutes, example: 12m), "
                        + "Nh (for hours, example: 2h).")
        @Default.String("5m")
        String getWindowDuration();

        void setWindowDuration(String value);

        @Description("The Avro Write Temporary Directory. Must end with /")
        @Validation.Required
        ValueProvider<String> getAvroTempDirectory();

        void setAvroTempDirectory(ValueProvider<String> value);
    }

    static class SamplerString extends DoFn<Long, String> {

        String schemaLocation;
        SchemaSampler sampler;
        String schema;
        SamplerString(String schemaLocation) {
            this.schemaLocation = schemaLocation;
        }

        @Setup
        public void setup() throws IOException {
            MatchResult.Metadata metadata = FileSystems.matchSingleFileSpec(schemaLocation);

            // Copy the schema file into a string which can be used for generation.
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                try (ReadableByteChannel readerChannel = FileSystems.open(metadata.resourceId())) {
                    try (WritableByteChannel writerChannel = Channels.newChannel(byteArrayOutputStream)) {
                        ByteStreams.copy(readerChannel, writerChannel);
                    }
                }
                schema = byteArrayOutputStream.toString();
                sampler = new SchemaSampler(schema);
            }
        }

        @DoFn.ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            Map<String, String> attributes = Maps.newHashMap();
            String data = sampler.sample().toString();
            //System.out.println(data);
            c.output(data);
        }
    }

    public static PipelineResult run(Options options) {

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> actor1 = pipeline.apply("ActorTrigger1", GenerateSequence.from(0L).withRate(options.getQps(), Duration.standardSeconds(1L))).apply("GenerateActor1Data", ParDo.of(new SamplerString("gs://deyhim-sandbox/sampler/actor4.json")));
        PCollection<String> actor2 = pipeline.apply("ActorTrigger2", GenerateSequence.from(0L).withRate(options.getQps(), Duration.standardSeconds(1L))).apply("GenerateActor2Data", ParDo.of(new SamplerString("gs://deyhim-sandbox/sampler/actor2.json")));
        PCollection<String> actor3 = pipeline.apply("ActorTrigger3", GenerateSequence.from(0L).withRate(options.getQps(), Duration.standardSeconds(1L))).apply("GenerateActor3Data", ParDo.of(new SamplerString("gs://deyhim-sandbox/sampler/actor3.json")));


        PCollectionList<String> actors = PCollectionList.of(actor1).and(actor2).and(actor3);
        PCollection<String> actorsFlattened = actors.apply(Flatten.<String>pCollections());
        PCollection<String> actorsDataWindow = actorsFlattened.apply("WindowData", Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))));

        actorsDataWindow.apply("WriteToPubSub",PubsubIO.writeStrings().to(options.getOutputTopic()));
        return pipeline.run();
    }

    public static void main(String[] args) throws IOException {


        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setStreaming(true);

        run(options);

    }
}
