package com.kafkaplusboltbeam;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.avatica.com.google.protobuf.Duration;
import org.joda.time.Duration;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
public class KafkaBoltBeam {

	public static void main(String[] args) {
		
		        PipelineOptions options = PipelineOptionsFactory.create();
		        Pipeline pipeline = Pipeline.create(options);

		        // Kafka producer configuration
		        String bootstrapServers = "localhost:8080"; // Change to your Kafka broker address
		        String topicName = "my-topic"; // Change to your Kafka topic name

		        KafkaIO.Read<String, String> kafkaSource = KafkaBolt.<String, String>read()
		                .withBootstrapServers(bootstrapServers)
		                .withTopics(Arrays.asList(topicName))
		                .withKeyDeserializer(StringDeserializer.class)
		                .withValueDeserializer(StringDeserializer.class)
		                .withTimestampPolicyFactory(TimestampPolicyFactory.withEventTime())
		                .withProcessingTime(Duration.standardSeconds(1));

		        // Read data from Kafka
		        PCollection<KafkaRecord<String, String>> kafkaData = pipeline.apply(kafkaSource);

		        // Use ParDo to send Kafka messages
		        PCollection<Void> kafkaSendResult = kafkaData.apply(ParDo.of(new DoFn<KafkaRecord<String, String>, Void>() {
		            @ProcessElement
		            public void processElement(@Element KafkaRecord<String, String> element, OutputReceiver<Void> out) {
		                String key = element.getKey();
		                String message = element.getValue();

		                // Create a Kafka record and send it (you'll need to configure your Kafka producer here)
		                ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", key, message);

		                producer.send(record, (metadata, exception) -> {
							if (exception == null) {
								// Message sent successfully
								System.out.println("Sent message with key '" + key + "' and value '" + message + "' to Kafka");
							} else {
								// Handle sending error
								LOG.error("Could not send message with key '" + key + "' and value '" + message + "' to Kafka",
										exception);
							}

		       
		        pipeline.run();
		    
	
)	};
		        
	}

}
	}
}
