import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class Client{

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private String kafkaServer = "localhost:9092";
    private static String topic = "topic_1";
    private int clientId;
    private KafkaConsumer kafkaConsumer;
    private KafkaProducer kafkaProducer;


    public Client(int clientId){
        this.clientId = clientId;
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(kafkaServer, topic);
        this.kafkaProducer = ProducerGenerator.generateProducer(kafkaServer);
    }

    public void produceMessages(String message) { //message should be a js code

        this.kafkaProducer.send(new ProducerRecord<String, String>(this.topic, message));
    }

    public void consumeMessage() {

        ScriptEngine nashormEngine = new ScriptEngineManager().getEngineByName("nashorn");

        try {
            while (true) {
                ConsumerRecords<String, String> records = this.kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.key()+ " "+ record.value());
                }
            }

        } catch(Exception exception) {
            LOGGER.error("Exception occured while consuing messages",exception);
        }finally {
            kafkaConsumer.close();

        }

        Runnable myRunnable =
                new Runnable(){
                    public void run(){
                        System.out.println("Runnable running");
                    }
                };
    }

    public static void main(String[] args) {
        final Client client = new Client(Integer.parseInt(args[0]));
        System.out.println(client.clientId);


//        client.consumeMessage();

//        assume the consensus scenario is a leader election
//        assume the number of node is 3
//        int clientsCount = 3;
//        assume every nodes know that consensus variable is c
//

        // Lambda Runnable
        Runnable consumming = new Runnable() {
            @Override
            public void run() {
                client.consumeMessage();
            }
        };
        new Thread(consumming).start();


        Runnable producing = new Runnable() {
            @Override
            public void run() {
                Scanner scanner = new Scanner(System.in);
                while(true){
                    String clientValue = scanner.next();
                    System.out.println(clientValue);
                    client.produceMessages(clientValue);

                }
            }
        };
        new Thread(producing).start();

//        }


    }
}

