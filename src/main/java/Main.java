public class Main {
    public static void main(String[] args){
//        Client.agreeOnValue(args[0]);
//        Client.electLeader(args[0], Integer.parseInt(args[1]));
        LockHandlerClient.handleLock(args[0]);
    }
}

