import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.graalvm.polyglot.*;

import org.graalvm.polyglot.Value;


public class LockHandlerClient extends Client {
    private static String topic = "consensus";
    private Boolean consensusAchieved = false;
//    private String code; // Js code that is updated in the runtime
//    private String evaluation; //Js code that evaluate the equality of variables that clients update
//    private String clientId;
//    private KafkaConsumer kafkaConsumer;
//    private KafkaProducer kafkaProducer;


    public LockHandlerClient(String clientId){
        super(clientId);
    }

    @Override
    public void consumeMessage() {

        org.graalvm.polyglot.Context jsContext = Context.create("js");

        try {
            while (!this.consensusAchieved) {
                ConsumerRecords<String, String> records = this.getKafkaConsumer().poll(5000);

                for (ConsumerRecord<String, String> record : records) {
                    this.setCode(this.getCode()+record.value());
//                    System.out.println(this.getCode());
                    Value result = jsContext.eval("js",this.getCode() + this.getEvaluation());

                    Boolean consensusResult = result.asBoolean();

                    if (!consensusResult){
                        this.produceMessages("lockStatuses.find((lockStatuses)=>{return lockStatuses.client ===\"" + this.getClientId() + "\";}).lockTaken = true;");
                        this.consensusAchieved = true;
                        for (int i=0; i <3; i++){
                            System.out.println("hi " + this.getClientId());
                            Thread.sleep(1000);
                        }
                        this.produceMessages("lockStatuses.find((lockStatuses) =>{return lockStatuses.lockTaken === true;}).lockTaken=false;");
                        break;
                    }
                    else{
                        System.out.println(true);
                    }
                }
            }

        } catch(Exception exception) {
            System.out.println("Exception occurred while reading messages"+ exception);
            exception.printStackTrace(System.out);
        }finally {
            this.getKafkaProducer().close();
        }
    }

    public static void handleLock(String clientId){
        final LockHandlerClient client = new LockHandlerClient(clientId);
        System.out.println(client.getClientId());

        client.setCode("var lockStatuses = []; result = false;");
        client.setEvaluation(
                "lockStatuses.forEach((lockStatus) => {" +
                    "result = result || lockStatus.lockTaken;" +
                "});" +
                "result;");

        client.produceMessages("lockStatuses.push({client:\""+ client.getClientId() + "\",lockTaken:false});");

        Runnable consuming = new Runnable() {
            @Override
            public void run() {
                client.consumeMessage();
            }
        };
        new Thread(consuming).start();
    }
}
