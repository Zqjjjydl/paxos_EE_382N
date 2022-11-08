package kvpaxos;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.atomic.AtomicInteger;

public class Client {
    String[] servers;
    int[] ports;

    // Your data here
    static final AtomicInteger NEXT_ID = new AtomicInteger(0);
    final int clientSeq = NEXT_ID.getAndIncrement();
    int opSeq;

    public Client(String[] servers, int[] ports){
        this.servers = servers;
        this.ports = ports;
        // Your initialization code here
        this.opSeq=0;
    }

    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id){
        Response callReply = null;
        KVPaxosRMI stub;
        try{
            Registry registry= LocateRegistry.getRegistry(this.ports[id]);
            stub=(KVPaxosRMI) registry.lookup("KVPaxos");
            if(rmi.equals("Get"))
                callReply = stub.Get(req);
            else if(rmi.equals("Put")){
                callReply = stub.Put(req);}
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }

    // RMI handlers
    public Integer Get(String key){
        // Your code here
        Op operation=new Op("Get",this.clientSeq,this.opSeq,key,-1);
        this.opSeq++;
        Request req=new Request(operation);
        for(int i=0;i<this.ports.length;i++){
            Response res=Call("Get", req, i);
            if(res!=null&&res.isSuccess){
                return res.value;
            }
        }
        return null;
    }

    public boolean Put(String key, Integer value){
        // Your code here
        Op operation=new Op("Put",this.clientSeq,this.opSeq,key,value);
        this.opSeq++;
        Request req=new Request(operation);
        for(int i=0;i<this.ports.length;i++){
            Response res=Call("Put", req, i);
            if(res!=null&&res.isSuccess){
                return true;
            }
        }
        return false;
    }

}
