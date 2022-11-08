package kvpaxos;
import paxos.Paxos;
import paxos.State;
// You are allowed to call Paxos.Status to check if agreement was made.

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class Server implements KVPaxosRMI {

    ReentrantLock mutex;
    Registry registry;
    Paxos px;
    int me;

    String[] servers;
    int[] ports;
    KVPaxosRMI stub;

    // Your definitions here
    ArrayList<Op> log;
    HashMap<String,Integer> stateMachine;
    int nextSeqIdx;

    public Server(String[] servers, int[] ports, int me){
        this.me = me;
        this.servers = servers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.px = new Paxos(me, servers, ports);
        // Your initialization code here
        this.log=new ArrayList<Op>();
        this.stateMachine=new HashMap<String,Integer>();
        this.nextSeqIdx=0;

        try{
            System.setProperty("java.rmi.server.hostname", this.servers[this.me]);
            registry = LocateRegistry.getRegistry(this.ports[this.me]);
            stub = (KVPaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("KVPaxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    //upon receiving a request
    //start a consensus on the operation on an instance seq id
    //if the value is same as initial, execute the op
    //else if the op is different while comes from the same client, means that current operation
    //        needs to be executed in next possible instance id. keep proposing until it finds a
    //        seq id larger than Max(), use Max+1 as new consensus execution
    //else if the op is different and comes from a different client, means that current operation
    //



    // RMI handlers
    public Response Get(Request req){
        // Your code here
        while(true){
            this.px.Start(this.nextSeqIdx,req.operation);
            Op decidedOperation=wait(this.nextSeqIdx);
            this.nextSeqIdx++;
            if(decidedOperation==null){
                break;
            }
            applyOperation(decidedOperation);
            if(decidedOperation.equals(req.operation)){
                return new Response(true,this.stateMachine.get(req.operation.key));
            }
        }
        return new Response(false,-1);
    }

    public Response Put(Request req){
        // Your code here
        while(true){
            this.px.Start(this.nextSeqIdx,req.operation);
            Op decidedOperation=wait(this.nextSeqIdx);
            this.nextSeqIdx++;
            if(decidedOperation==null){
                break;
            }
            applyOperation(decidedOperation);
            if(decidedOperation.equals(req.operation)){
                return new Response(true,-1);
            }
        }
        return new Response(false,-1);
    }

    public Op wait(int seq){
        int to=10;
        for(int i=0;i<40;i++){
            Paxos.retStatus ret=this.px.Status(seq);
            if(ret.state==State.Decided){
                return Op.class.cast(ret.v);
            }
            try{
                Thread.sleep(to);
            } catch(Exception e){
                e.printStackTrace();
            }
            if(to<1000){
                to=to*2;
            }
        }
        return null;
    }

    void applyOperation(Op operation){
        if(operation.op.equals("Put")){
            this.stateMachine.put(operation.key,operation.value);
        }
    }

}
