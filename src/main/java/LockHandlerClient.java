import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.graalvm.polyglot.*;

import org.graalvm.polyglot.Value;


public class LockHandlerClient extends Client {
    private Boolean consensusAchieved = false;

    public LockHandlerClient(String clientId){
        super(clientId);
    }

    @Override
    public void consumeMessage() {

//        this.getKafkaConsumer().seekToBeginning(this.getKafkaConsumer().assignment());
//        instead of seeking to beginning consumer configuration is set to "auto.offset.reset", "earliest"
        org.graalvm.polyglot.Context jsContext = Context.create("js");

        try {
            while (!this.consensusAchieved) {
                ConsumerRecords<String, String> records = this.getKafkaConsumer().poll(5);

                for (ConsumerRecord<String, String> record : records) {
                    this.setCode(this.getCode()+record.value());
                    Value result = jsContext.eval("js",this.getCode() + this.getEvaluation());

                    boolean consensusResult = result.asBoolean();

                    if (consensusResult){
                        int iterations = (int)(3 + Math.random()*10);
                        this.consensusAchieved = true;
                        for (int i = 0; i<iterations; i++){
                            System.out.println(this.getClientId());
                            Thread.sleep(1000);
                        }
                        this.produceMessages("lockStatuses.delete(\""+ this.getClientId() + "\"" + ");");
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
            this.getKafkaConsumer().close();
        }
    }

    public static void handleLock(String clientId){
        final LockHandlerClient client = new LockHandlerClient(clientId);

        client.setCode("var lockStatuses = new Set([]); result = false;");
        client.setEvaluation(
                "console.log(\"queue is :\" + Array.from(lockStatuses));" +
                "if(Array.from(lockStatuses)[0] === \"" + client.getClientId() + "\"){" +
                    "result = true;" +
                "}" +
                "result;");

        client.produceMessages("lockStatuses.add(\""+ client.getClientId() + "\"" + ");");

        Runnable consuming = new Runnable() {
            @Override
            public void run() {
                client.consumeMessage();
            }
        };
        new Thread(consuming).start();
    }
}


