import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.script.ScriptException;

import org.graalvm.polyglot.*;

public class Client {

    private static String topic = "consensus";
    private Boolean consensusAchieved = false;
    private String code; // Js code that is updated in the runtime
    private String evaluation; //Js code that evaluate the equality of variables that clients update
    private String clientId;
    private KafkaConsumer kafkaConsumer;
    private KafkaProducer kafkaProducer;

    public String getCode() {
        return code;
    }

    public String getEvaluation() {
        return evaluation;
    }

    public Client(String clientId){
        //clientId is also the variable that a client instance update until each client comes to same value
        // also clientId is user as the consumer group is of the client so that kafka broadcast each message to every client
        this.clientId = clientId;
        String kafkaServer = "localhost:9092";
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(kafkaServer, topic, this.clientId);
        this.kafkaProducer = ProducerGenerator.generateProducer(kafkaServer);
    }

    public String getClientId() {
        return clientId;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public void produceMessages(String message) { //message should be a js code
        this.kafkaProducer.send(new ProducerRecord<String, String>(topic, message));
    }


    public void consumeMessage() {

        org.graalvm.polyglot.Context jsContext = Context.create("js");

        try {
            while (!this.consensusAchieved) {
                ConsumerRecords<String, String> records = this.kafkaConsumer.poll(10);

                for (ConsumerRecord<String, String> record : records) {
                    this.code += record.value();
                    Value result = jsContext.eval("js",this.code + this.evaluation);


                    Boolean consensusResult = result.getMember("consensus").asBoolean();
                    Value agreedValue = result.getMember("value");


                    if (consensusResult){
                        this.consensusAchieved = true;
                        System.out.println("Consensus.");
                        System.out.println(agreedValue);
                        Thread.sleep(5000);
                    }
                    else{
                        System.out.println(false);
                    }
                }
            }

        } catch(Exception exception) {
            System.out.println("Exception occurred while reading messages"+ exception);
            exception.printStackTrace(System.out);
        }finally {
            kafkaConsumer.close();
        }
    }

    public KafkaConsumer getKafkaConsumer() {
        return kafkaConsumer;
    }

    public KafkaProducer getKafkaProducer() {
        return kafkaProducer;
    }

    public static void agreeOnValue(String clientId){
        final Client client = new Client(clientId);
        System.out.println("Client id is " + client.clientId);


//        assume the number of node is 3
//        three client instances are trying to agree on a value
        client.code = "var x=null;y=null;z=null; result = {consensus:false, value:\"null\"};";
        client.evaluation =
                "if(x===y && y===z && x!==null){" +
                    "result.consensus=true;" +
                    "result.value=x;" +
                "}" +
                "result;";

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

                while(!client.consensusAchieved){

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

    public void setEvaluation(String evaluation) {
        this.evaluation = evaluation;
    }

    public static void electLeader(String clientId, int instanceCount) {
        final Client client = new Client(clientId);
        System.out.println(client.clientId);

        client.code  = "var clientRanks = []; result = {consensus:false, value:\"null\"};";
        client.evaluation =
                "if(Object.keys(clientRanks).length==" + instanceCount + "){" +
                    "var leader = null;"+
                    "var maxRank = 0;"+
                    "for (var i = 0; i < clientRanks.length; i++) {"+
                        "if(clientRanks[i].rank > maxRank){"+
                            "result.consensus=true;" +
                            "result.value = clientRanks[i].client;" +
                            "maxRank = clientRanks[i].rank;" +
                        " }" +
                    "}" +
                "}" +
                "result;";

        Runnable consuming = new Runnable() {
            @Override
            public void run() {
                client.consumeMessage();
            }
        };
        new Thread(consuming).start();

        int clientRank = (int)(1 + Math.random()*100);
        client.produceMessages("clientRanks.push({client:\""+ client.clientId + "\",rank:" + clientRank +"});");
        System.out.println(clientRank);
    }
}

