/*
 * Copyright 2016 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package testRegistry;

import com.esotericsoftware.minlog.Log;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;

/**
 * Below class can be used to send messages to a given topic in kafka-producer.props like below.
 *
 * KafkaAvroSerDesApp -sm -d yelp_review_json -s yelp_review.avsc -p kafka-producer.props
 * KafkaAvroSerDesApp -sm -d hwxavroschema1_json -s HwxAvroSchema1.avsc -p kafka-producer.props
 * or
 * KafkaAvroSerDesApp -cm  -c kafka-consumer.props
 * If invalid messages need to be ignored while sending messages to a topic, you can set "ignoreInvalidMessages" to true
 * in kafka producer properties file.
 *
 */
public class KafkaAvroSerDesApp {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroSerDesApp.class);

	public static final String MSGS_LIMIT_OLD = "msgsLimit";
	public static final String MSGS_LIMIT = "msgs.limit";
	public static final String TOPIC = "topic";
	public static final int DEFAULT_MSGS_LIMIT = 50;
	public static final String IGNORE_INVALID_MSGS = "ignore.invalid.messages";

	private String producerProps,schemaFile,consumerProps;

	public KafkaAvroSerDesApp(String producerProps, String schemaFile) {
		this.producerProps = producerProps;
		this.schemaFile = schemaFile;
	}

	public KafkaAvroSerDesApp(String consumerProps) {
		this.consumerProps = consumerProps;
	}

	public void sendMessages(String payloadJsonFile) throws Exception {

		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		Properties props = new Properties();
		try(InputStream resourceStream = loader.getResourceAsStream(producerProps)) {
			props.load(resourceStream);
		}

		int limit = Integer.parseInt(props.getProperty(MSGS_LIMIT_OLD,
				props.getProperty(MSGS_LIMIT,
						DEFAULT_MSGS_LIMIT + "")));
		boolean ignoreInvalidMsgs = Boolean.parseBoolean(props.getProperty(IGNORE_INVALID_MSGS, "false"));

		int current = 0;
		File currentDir = new File("."); // current directory
		displayDirectoryContents(currentDir);
		Schema schema = new Schema.Parser().parse(new File("C:\\Users\\VenkataW\\eclipse-workspace\\testRegistry\\src\\main\\resources\\yelp_review.avsc"));
		String topicName = props.getProperty(TOPIC);

		// set protocol version to the earlier one.
		props.put(SERDES_PROTOCOL_VERSION, SerDesProtocolHandlerRegistry.CONFLUENT_VERSION_PROTOCOL);
		final Producer<String, Object> producer = new KafkaProducer<>(props);
		final Callback callback = new MyProducerCallback();

		try (BufferedReader bufferedReader = new BufferedReader(new FileReader("C:\\Users\\VenkataW\\eclipse-workspace\\testRegistry\\src\\main\\resources\\yelp_review_json"))) {
			String line;
			while (current++ < limit && (line = bufferedReader.readLine()) != null) {
				// convert json to avro records
				Object avroMsg = null;
				try {
					avroMsg = jsonToAvro(line, schema);
					//AvroMsg= {"user_id": "PUFPaY9KxDAcGqfsorJp3Q", "review_id": "Ya85v4eqdd6k9Od8HbQjyA", "stars": 4, "date": "2012-08-01", "business_id": "5UmKMjUEUNdYWqANhGckJw", "type": "review", "votes": {"funny": 0, "useful": 0, "cool": 0}}
					Log.info(avroMsg.getClass().toString());
				} catch (Exception ex) {
					LOG.warn("Error encountered while converting json to avro of message [{}]", line, ex);
					if(ignoreInvalidMsgs) {
						continue;
					} else {
						throw ex;
					}
				}

				// send avro messages to given topic using KafkaAvroSerializer which registers payload schema if it does not exist
				// with schema name as "<topic-name>:v", type as "avro" and schemaGroup as "kafka".
				// schema registry should be running so that KafkaAvroSerializer can register the schema.
				LOG.info("Sending message: [{}] to topic: [{}]", avroMsg, topicName);
				ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topicName, avroMsg);
				try {
					producer.send(producerRecord, callback);
				} catch (SerDesException ex) {
					LOG.warn("Error encountered while sending message [{}]", line, ex);
					if(!ignoreInvalidMsgs) {
						throw ex;
					}
				}
			}
		} finally {
			producer.flush();
			LOG.info("All message are successfully sent to topic: [{}]", topicName);
			producer.close(5, TimeUnit.SECONDS);
		}
	}

	private Object jsonToAvro(String jsonString, Schema schema) throws Exception {
		DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
		Object object = reader.read(null, DecoderFactory.get().jsonDecoder(schema, jsonString));

		if (schema.getType().equals(Schema.Type.STRING)) {
			object = object.toString();
		}
		return object;
	}
	public static void displayDirectoryContents(File dir) {
		try {
			File[] files = dir.listFiles();
			for (File file : files) {
				if (file.isDirectory()) {
					System.out.println("directory:" + file.getCanonicalPath());
					displayDirectoryContents(file);
				} else {
					System.out.println("     file:" + file.getCanonicalPath());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private static class MyProducerCallback implements Callback {
		@Override
		public void onCompletion(RecordMetadata recordMetadata, Exception e) {
			LOG.info("#### received [{}], ex: [{}]", recordMetadata, e);
		}
	}

	public void consumeMessages() throws Exception {
		Properties props = new Properties();
		//String resourceName = "kafka-consumer.props"; // could also be a constant
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		try(InputStream resourceStream = loader.getResourceAsStream(this.consumerProps)) {
			props.load(resourceStream);
		}
		String topicName = props.getProperty(TOPIC);
		@SuppressWarnings("resource")
		KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(topicName));
		/*TopicPartition tp=new TopicPartition(topicName,0);
        consumer.assign(Arrays.asList(tp));*/

		consumer.subscribe(Arrays.asList(topicName));
		consumer.poll(10);
		consumer.seek(new TopicPartition(topicName,0),0);

		while (true) {
			ConsumerRecords<String, Object> records = consumer.poll(1000);
			LOG.info("records size " + records.count());
			for (ConsumerRecord<String, Object> record : records) {
				LOG.info("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
			}
		}
	}

	/**
	 * Print the command line options help message and exit application.
	 */
	@SuppressWarnings("static-access")
	private static void showHelpMessage(String[] args, Options options) {
		Options helpOptions = new Options();
		helpOptions.addOption(Option.builder("h").longOpt("help")
				.desc("print this message").build());
		try {
			CommandLine helpLine = new DefaultParser().parse(helpOptions, args, true);
			if (helpLine.hasOption("help") || args.length == 1) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("truck-events-kafka-ingest", options);
				System.exit(0);
			}
		} catch (ParseException ex) {
			LOG.error("Parsing failed.  Reason: " + ex.getMessage());
			System.exit(1);
		}
	}

	public static void main(String[] args) throws Exception {
		if (System.getProperty("java.security.auth.login.config") == null) {
			System.setProperty("java.security.auth.login.config",
					"C:\\Users\\VenkataW\\eclipse-workspace\\testRegistry\\src\\main\\resources\\jaas.conf");
		}

		Option sendMessages = Option.builder("sm").longOpt("send-messages").desc("Send Messages to Kafka").type(Boolean.class).build();
		Option consumeMessages = Option.builder("cm").longOpt("consume-messages").desc("Consume Messages from Kafka").type(Boolean.class).build();


		Option dataFileOption = Option.builder("d").longOpt("data-file").hasArg().desc("Provide a data file").type(String.class).build();
		Option producerFileOption = Option.builder("p").longOpt("producer-config").hasArg().desc("Provide a Kafka producer config file").type(String.class).build();
		Option schemaOption = Option.builder("s").longOpt("schema-file").hasArg().desc("Provide a schema file").type(String.class).build();

		Option consumerFileOption = Option.builder("c").longOpt("consumer-config").hasArg().desc("Provide a Kafka Consumer config file").type(String.class).build();

		OptionGroup groupOpt = new OptionGroup();
		groupOpt.addOption(sendMessages);
		groupOpt.addOption(consumeMessages);
		groupOpt.setRequired(true);

		Options options = new Options();
		options.addOptionGroup(groupOpt);
		options.addOption(dataFileOption);
		options.addOption(producerFileOption);
		options.addOption(schemaOption);
		options.addOption(consumerFileOption);

		//showHelpMessage(args, options);

		CommandLineParser parser = new DefaultParser();
		CommandLine commandLine;

		try {
			commandLine = parser.parse(options, args);
			if (commandLine.hasOption("sm")) {
				if (commandLine.hasOption("p") && commandLine.hasOption("d") && commandLine.hasOption("s")) {
					KafkaAvroSerDesApp kafkaAvroSerDesApp = new KafkaAvroSerDesApp(commandLine.getOptionValue("p"),
							commandLine.getOptionValue("s"));
					kafkaAvroSerDesApp.sendMessages(commandLine.getOptionValue("d"));
				} else {
					LOG.error("please provide following options for sending messages to Kafka");
					LOG.error("-d or --data-file");
					LOG.error("-s or --schema-file");
					LOG.error("-p or --producer-config");
				}
			} else if (commandLine.hasOption("cm")) {
				if (commandLine.hasOption("c")) {
					KafkaAvroSerDesApp kafkaAvroSerDesApp = new KafkaAvroSerDesApp(commandLine.getOptionValue("c"));
					kafkaAvroSerDesApp.consumeMessages();
				} else {
					LOG.error("please provide following options for consuming messages from Kafka");
					LOG.error("-c or --consumer-config");
				}
			}
		} catch (ParseException e) {
			LOG.error("Please provide all the options ", e);
		} catch (Exception e) {
			LOG.error("Failed to send/receive messages ", e);
		}

	}
}
