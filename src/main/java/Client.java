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

public class Client {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private String kafkaServer = "localhost:9092";
    private String serealizer = "org.apache.kafka.common.serialization.StringSerializer";
    private String deserealizer = "org.apache.kafka.common.serialization.StringDeserializer";


    public void ProdocuMessages(String message){
        Properties props=new Properties();
        props.put("bootstrap.servers", this.kafkaServer);
        props.put("key.serializer", this.serealizer );
        props.put("value.serializer", this.serealizer);

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
        props.put("key.deserializer", this.deserealizer);
        props.put("value.deserializer", this.deserealizer);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        ScriptEngine nashormEngine;
        nashormEngine = new ScriptEngineManager().getEngineByName("nashorn");
        nashormEngine.eval(new FileReader("src/main/java/script.js"));
        Invocable invocableNashormEngine = (Invocable) nashormEngine;

        consumer.subscribe(Arrays.asList("topic1"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("key :" + record.key());
                    System.out.println("value :" + record.value());
                    System.out.println("topic :" + record.topic());
                    System.out.println("partition :" + record.partition());
                    System.out.println(invocableNashormEngine.invokeFunction("func1"));
                }
            }

        } catch(Exception e) {
            LOGGER.error("Exception occured while consuing messages",e);
        }finally {
            consumer.close();
        }

    }

    public static void main(String args[]) throws FileNotFoundException, ScriptException {
        Client sampleClient = new Client();
        sampleClient.ProdocuMessages("var int_array = Java.type(\"int[]\");var a = new int_array(4);a[0] = 3;a[1] = 32;a[2] = 1;a[3] = 4;var total = 0;for (i = 0; i<a.length; i++){total += a[i]}return total;");
        sampleClient.consumeMessage();
    }
}

