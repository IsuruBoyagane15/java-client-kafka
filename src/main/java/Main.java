import javax.script.ScriptException;

public class Main {
    public static void main(String[] args){
//        Client.agreeOnValue(args[0]);
        try {
//            Client.electLeader("c1", 1);
            Client.electLeader(args[0], Integer.parseInt(args[1]));
        } catch (ScriptException e) {
            e.printStackTrace();
        }
    }
}

