import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Client{

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private String kafkaServer = "localhost:9092";


    public void ProduceMessages(String message){
        Properties props=new Properties();
        props.put("bootstrap.servers", this.kafkaServer);
        String serializer = "org.apache.kafka.common.serialization.StringSerializer";
        props.put("key.serializer", serializer);
        props.put("value.serializer", serializer);

        KafkaProducer<String,String> sampleProducer= new KafkaProducer<String,String>(props);

        sampleProducer.send(new ProducerRecord<String, String>("topic1", message));
        sampleProducer.close();
    }

    public void consumeMessage() throws FileNotFoundException, ScriptException {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.kafkaServer);
        props.put("group.id", "topic1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        String deserializer = "org.apache.kafka.common.serialization.StringDeserializer";
        props.put("key.deserializer", deserializer);
        props.put("value.deserializer", deserializer);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        ScriptEngine nashormEngine;
        nashormEngine = new ScriptEngineManager().getEngineByName("nashorn");
        nashormEngine.eval(new FileReader("src/main/java/script.js"));
//        Invocable invocableNashormEngine = (Invocable) nashormEngine;

        consumer.subscribe(Arrays.asList("topic1"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
//                    System.out.println("key :" + record.key());
                    System.out.println(nashormEngine.eval(record.value()));
//                    System.out.println("topic :" + record.topic());
//                    System.out.println("partition :" + record.partition());
//                    System.out.println(invocableNashormEngine.invokeFunction("func1"));
                }
            }

        } catch(Exception e) {
            LOGGER.error("Exception occured while consuing messages",e);
        }finally {
            consumer.close();
        }

    }


    public static void main(String args[]) throws FileNotFoundException, ScriptException {
        Client sampleClient1 = new Client();
        Client sampleClient2 = new Client();

        sampleClient1.ProduceMessages("var a = 1; var b = 0; a+b;");
        sampleClient2.ProduceMessages("var b = 3; a+b;");
//        sampleClient2.consumeMessage();
        sampleClient1.consumeMessage();
    }
}

