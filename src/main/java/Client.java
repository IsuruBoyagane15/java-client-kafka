import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class Client{

    private String kafkaServer = "localhost:9092";
    private static String topic = "con";
    private Boolean consensusAchieved = false;
    private String code; // Js code that is updated in the runtime
    private String evaluation; //Js code that evaluate the equality of variables that clients update
    private String clientId;
    private KafkaConsumer kafkaConsumer;
    private KafkaProducer kafkaProducer;


    public Client(String clientId){
        //clientId is also the variable that a client instance update until each client comes to same value
        // also clientId is user as the consumer group is of the client so that kafka broadcast each message to every client
        this.clientId = clientId;
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(kafkaServer, topic, this.clientId);
        this.kafkaProducer = ProducerGenerator.generateProducer(kafkaServer);
    }


    public void produceMessages(String message) { //message should be a js code

        this.kafkaProducer.send(new ProducerRecord<String, String>(topic, message));
    }

    public void consumeMessage() {

        ScriptEngine nashorn = new ScriptEngineManager().getEngineByName("nashorn");

        try {
            while (this.consensusAchieved == false) {
                ConsumerRecords<String, String> records = this.kafkaConsumer.poll(10);
                for (ConsumerRecord<String, String> record : records) {

                    this.code += record.value();
                    Object result = nashorn.eval(this.code + this.evaluation);

                    int integerResult = (Integer)result;
                    if (integerResult == 1){
                        this.consensusAchieved = true;
                        System.out.println("Consensus.");
                    }
                    else{
                        System.out.println(false);
                    }
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


//        assume the number of node is 3
//        three client instances are trying to agree on a value
        client.code = "var x=null;y=null;z=null;";
        client.evaluation = "if(x===y && y===z && x!==null){1;}else{0;}";


        // Lambda Runnable
        Runnable consuming = new Runnable() {
            @Override
            public void run() {
                client.consumeMessage();
            }
        };
        new Thread(consuming).start();

        // Lambda Runnable
        Runnable producing = new Runnable() {
            @Override
            public void run() {

                while(client.consensusAchieved == false){

                    int clientRandom = (int)(1 + Math.random()*3);
                    System.out.println(client.clientId + "=" + clientRandom);
                    String clientValue = client.clientId + "=" + clientRandom + ";"; //generate js line to write into kafka
                    client.produceMessages(clientValue);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        new Thread(producing).start();
    }
}

