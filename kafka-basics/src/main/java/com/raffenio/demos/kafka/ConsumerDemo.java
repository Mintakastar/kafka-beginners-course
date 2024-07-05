package com.raffenio.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


/**

 codigo para crear la particion por CLI:

    kafka-topics.sh --bootstrap-server localhost:9092 --topic demo_java --create --partitions 3

 listar los topicos:
     kafka-topics.sh --bootstrap-server localhost:9092 --list

 consumer el mensaje por CLI y validar que si se mando
    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo_java
    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo_java --from-beginning


 */
public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I'm a kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";


        //create producer properties
        Properties properties = new Properties();
        //connecto to localhost
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        //set producer properties
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        //properties.setProperty("value.serializer",StringSerializer.class.getName());


        //craete consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());

        properties.setProperty("group.id",groupId);
        properties.setProperty("auto.offset.reset","earliest");// 3 possible values:   none (if no have existing consumer group then we fail)    /     earliest ( read from begining ) /  latest  (read from now)

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic ));

        //poll for data
        while(true){
            log.info("polling");


            ConsumerRecords<String,String> records =consumer.poll(Duration.ofMillis(1000));//wait one second to next poll

            for (ConsumerRecord<String,String> record : records ){
                log.info("Key "+record.key()+", value: "+record.value()+ "\t PArtition: "+record.partition()+", Offset: "+record.offset());
            }
        }



    }
}
