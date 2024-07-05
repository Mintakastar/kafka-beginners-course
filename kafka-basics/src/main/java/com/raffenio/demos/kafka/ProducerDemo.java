package com.raffenio.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("hello world");

        //create producer properties
        Properties properties = new Properties();
        //connecto to localhost
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        //connect to condukcor playground
        /*properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jass.config",""); //info from conektor
        properties.setProperty("sasl.mechanism","PLAIN");*/

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        //crate producer
        KafkaProducer <String,String> producer = new KafkaProducer<>(properties);

        //create a producer record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","hello world"); //topic value


        //send data
        producer.send(producerRecord);

        //flush and close the producer
        //tell the producer to send all data and block until done. SYNCHRONOUS
        producer.flush();

        //flush ad close  the producer
        producer.close();



    }
}
