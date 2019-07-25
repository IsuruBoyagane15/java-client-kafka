import java.util.Scanner;

import com.itextpdf.kernel.counter.SystemOutEventCounter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class Client{

    private String kafkaServer = "localhost:9092";
    private static String topic = "con";
    private String code;
    private String evaluation;
    private String clientId;
    private KafkaConsumer kafkaConsumer;
    private KafkaProducer kafkaProducer;


    public Client(String clientId){
        this.clientId = clientId;
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(kafkaServer, topic, this.clientId);
        this.kafkaProducer = ProducerGenerator.generateProducer(kafkaServer);
    }

    public void produceMessages(String message) { //message should be a js code

        this.kafkaProducer.send(new ProducerRecord<String, String>(topic, message));
    }

    public void consumeMessage() {

        ScriptEngine nashormEngine = new ScriptEngineManager().getEngineByName("nashorn");

        try {
            while (true) {
                ConsumerRecords<String, String> records = this.kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    this.code += record.value();
//                    System.out.println(record.partition());
                    System.out.println(nashormEngine.eval(this.code + this.evaluation));
//                    System.out.println((this.code + this.evaluation));

                }
            }

        } catch(Exception exception) {
            System.out.println("Exception occurred while reading messages"+ exception);
        }finally {
            kafkaConsumer.close();

        }

    }

    public static void main(String[] args) {
        final Client client = new Client(args[0]);
        System.out.println(client.clientId);


//        client.consumeMessage();

//        assume the consensus scenario is a leader election
//        assume the number of node is 3
        client.code = "var x=null;y=null;z=null;";
        client.evaluation = "if(x===y && y===z){true;}else{false;}";
//        int clientsCount = 3;
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
                    client.produceMessages(clientValue);

                }
            }
        };
        new Thread(producing).start();

//

    }
}

