package com.raffenio.demos.kafka;

import org.apache.kafka.clients.producer.*;
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
public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I'm a kafka producer!");

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

        //properties.setProperty("batch.size","400");//small size to change partitions  //not to use in prod
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());//not to use in prod

        //crate producer
        KafkaProducer <String,String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {
            log.info("------------------------------------------");
            for (int i = 0; i < 10; i++) {
                String topic= "demo_java";
                String key= "id_"+i;
                String value = "hello World "+i;

                //create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,key, value); //topic value
                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            //the recoerd was successfully sent
                            log.info("Key: " + key+ "\t" +          "Partition: " + recordMetadata.partition() /*+ "\t" +          "Offset: " + recordMetadata.offset() + "\t" + "TimeStamp: " + recordMetadata.timestamp() + ""*/);
                        } else {
                            log.error("Error while producing.", e);
                        }
                    }//end callback
                });//end send
            }//first for
            log.info("------------------------------------------");
            try {  Thread.sleep(500);  }catch (Exception e){  log.error("error al dormir",e); }

        }


        //flush and close the producer
        //tell the producer to send all data and block until done. SYNCHRONOUS
        producer.flush();

        //flush ad close  the producer
        producer.close();



    }
}
