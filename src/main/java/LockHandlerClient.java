import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.graalvm.polyglot.*;

import org.graalvm.polyglot.Value;


public class LockHandlerClient extends Client {
    private static String topic = "consensus";
    private Boolean consensusAchieved = false;

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
                    Value result = jsContext.eval("js",this.getCode() + this.getEvaluation());

                    Boolean consensusResult = result.asBoolean();

                    if (consensusResult){
                        this.consensusAchieved = true;
                        for (int i = 0; i<15; i++){
                            System.out.println(this.getClientId());
                            Thread.sleep(1000);
                        }
                        this.produceMessages("lockStatuses.shift();");
                        break;
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
            this.getKafkaProducer().close();
        }
    }

    public static void handleLock(String clientId){
        final LockHandlerClient client = new LockHandlerClient(clientId);

        client.setCode("var lockStatuses = []; result = false;");
        client.setEvaluation(
                "if(lockStatuses[0] === \"" + client.getClientId() + "\"){\n" +
                    "result = true;\n" +
                "}\n" +
                "result;\n");

        client.produceMessages("lockStatuses.push(\""+ client.getClientId() + "\"" + ");");

        Runnable consuming = new Runnable() {
            @Override
            public void run() {
                client.consumeMessage();
            }
        };
        new Thread(consuming).start();
    }
}


