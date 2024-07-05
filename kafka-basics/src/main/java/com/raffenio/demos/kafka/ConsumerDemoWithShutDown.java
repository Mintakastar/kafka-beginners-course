package com.raffenio.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
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
public class ConsumerDemoWithShutDown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDown.class.getSimpleName());
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


        //add a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        //add shut down hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("1 Detected a shutdown let's exist by calling the consumer.wakeup()..");
                consumer.wakeup();
                log.info("2");
                //waiting for the main program to finish

                //join the main thread to allow the execution of the code in the main thread
                try {
                 mainThread.join();
                    log.info("3");
                } catch (Exception e){
                   log.error(e.getMessage(),e);
                }
                log.info("4");

            }
        });

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic ));

        try {
            //poll for data
            while(true){
                //log.info("polling");
                ConsumerRecords<String,String> records =consumer.poll(Duration.ofMillis(1000));//wait one second to next poll  //will throw a wake up exception

                for (ConsumerRecord<String,String> record : records ){
                    log.info("Key "+record.key()+", value: "+record.value()+ "\t PArtition: "+record.partition()+", Offset: "+record.offset());
                }
            }
        } catch (WakeupException e){
           log.info("Cosumer is starting to shudown");
        }catch (Exception e){
            log.error("unexpected exception in cosumer ",e);
        }finally {
            consumer.close();//close the consumer, this will commit the offset
            log.info("the consumer is nwo gracefully shut down");
        }




    }
}
