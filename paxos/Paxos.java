package paxos;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is the main class you need to implement paxos instances.
 */
public class Paxos implements PaxosRMI, Runnable{

    ReentrantLock mutex;
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing

    // Your data here
    /*
    This class contains the Proposal Instance information
    One seq number is corresponding to one agreement instance
     */
    public class Instance {
        int minProposal;
        int highestAcceptedProposal;
        Object value;
        State state; // status of the current proposal

        public Instance(int minProposal,int highestAcceptedProposal, Object value) {
            this.minProposal = minProposal;  // the highest prepare seen
            this.highestAcceptedProposal=highestAcceptedProposal;
            this.value = value;
            state = State.Pending;
        }
    }

    public class proposerParameter{
        int seqId;
        Object value;

        public proposerParameter(int seqId,Object value){
            this.seqId=seqId;
            this.value=value;
        }
    }

    Map<Integer, Instance> instanceMap; // agreements
    HashMap<Long,proposerParameter> threadId2ProposerParameter;//key is thread name, value is proposer parameter, including seq id and value
    int peersNum; // number of peers
    int majority; // number of majority

    //these variables are replaced by threadname2ProposerParameter
//    int curSeq; // current sequence
//    Object curVal;
    int[] highestDoneSeq; // record the highest number ever passed to Done() on all peers

    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports){

        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);

        // Your initialization code here
        peersNum = peers.length;
        majority = peersNum / 2 + 1;
        instanceMap = new HashMap<>();
        threadId2ProposerParameter=new HashMap<Long,proposerParameter>();
        highestDoneSeq = new int[peersNum];
        Arrays.fill(highestDoneSeq, -1);

        // register peers, do not modify this part
        try {
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     * <p>
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     * <p>
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id) {
        Response callReply = null;

        PaxosRMI stub;
        try {
            Registry registry = LocateRegistry.getRegistry(this.ports[id]);
            stub = (PaxosRMI) registry.lookup("Paxos");
            if (rmi.equals("Prepare"))
                callReply = stub.Prepare(req);
            else if (rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if (rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch (Exception e) {
            return null;
        }
        return callReply;
    }


    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     * <p>
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     * <p>
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */
    public void Start(int seq, Object value) {
        // Your code here

        // start a new thread
        Thread thread = new Thread(this,Integer.toString(seq));
        mutex.lock();
        try {
            // reset req and v in Paxos object, and pass it to the new Thread
            this.threadId2ProposerParameter.put(thread.getId(),new proposerParameter(seq,value));
        } finally {
            mutex.unlock();
        }
        thread.start();
    }

    /**
     * In this run function, server lead the phase 1 (prepare) and
     * phase 2 (accept) of the algorithm as a proposer. The server send prepare
     * and accept request to the acceptors.
     */
    @Override
    public void run() {
        //Your code here
        int proposalNum = 0;
        // increase in the loop
        int highestNumSeen = 0;
        System.out.println("Start a new Paxos Thread");
        proposerParameter proposerParameter=this.threadId2ProposerParameter.get(Thread.currentThread().getId());
        int curSeq=proposerParameter.seqId;
        Object curVal=proposerParameter.value;
        while (!this.isDead()&&Status(curSeq).state != State.Decided) {
            if (curSeq < Min()) {
                return;
            }
            /* ------------------ phase 1: Prepare ------------------ */
            // choose a unique and higher proposal number
            proposalNum = ((highestNumSeen + 1 + this.peersNum)/this.peersNum)*this.peersNum + me;
            highestNumSeen = proposalNum;
            // sent prepare(n) to all servers and get the Response
            Response[] responses = new Response[peersNum];

            Request newReq = new Request(curSeq, proposalNum, null, me, highestDoneSeq[me]);
            for (int id = 0; id < this.peersNum; id++) {
                // local peer: no need to send rmi call
                if (id == me) {
                    responses[id] = Prepare(newReq);
                } else {
                    responses[id] = Call("Prepare", newReq, id);
                }
            }
            int ackCount = 0;
            int highestNumAccepted = Integer.MIN_VALUE;
            int highestId = -1;
            for (int id = 0; id < responses.length; id++) {
                Response response = responses[id];
                if (response == null) {
                    continue;
                }
                highestNumSeen = Math.max(highestNumSeen, Math.max(response.numberAccepted, response.proposalNumber));
                if (response.ack) {
                    ackCount++;
                }
                // find the highest accepted proposal number
                if (response.numberAccepted > highestNumAccepted) {
                    highestNumAccepted = response.numberAccepted;
                    highestId = id;
                }
            }
            Object sentValue = curVal;
            if (ackCount < majority) {
                continue;
            } else {
                // if any NumAccepted received
                if (highestNumAccepted >= 1) {
                    sentValue = responses[highestId].valueAccepted;
                }
                /* ------------------ phase 2: Accept ------------------ */
                newReq = new Request(curSeq, proposalNum, sentValue, me, highestDoneSeq[me]);
                for (int id = 0; id < peersNum; id++) {
                    if (id == me) {
                        responses[id] = Accept(newReq);
                    } else {
                        responses[id] = Call("Accept", newReq, id);
                    }
                }
            }
            // /* ------------------ phase 3:  ------------------ */
            ackCount = 0;
            for (Response response : responses) {
                if (response == null) {
                    continue;
                }
                highestNumSeen = Math.max(highestNumSeen, Math.max(response.numberAccepted, response.proposalNumber));
                if (response.ack) {
                    ackCount++;
                }
            }
            boolean continueFlag = false;
            if (ackCount < majority) {
                continue;
            } else {
                for (Response response : responses) {
                    if (response == null) {
                        continue;
                    }
                    highestNumSeen = Math.max(highestNumSeen, Math.max(response.numberAccepted, response.proposalNumber));
                    if (!response.ack && response.proposalNumber > proposalNum) {
                        continueFlag = true;
                        break;
                    }
                }
                if (continueFlag) {
                    continue;
                }
                newReq = new Request(curSeq, proposalNum, sentValue, me, highestDoneSeq[me]);
                for (int id = 0; id < peersNum; id++) {
                    if (id == me) {
                        responses[id] = Decide(newReq);
                    } else {
                        responses[id] = Call("Decide", newReq, id);
                    }
                }
            }
        }

    }

    /**
     * Acceptor handle the prepare request and give respond to the Proposer
     *
     * @param req req(seq, proposalNumber, valueAccepted)
     * @return respond to the prepare request
     */
    // RMI handler
    public Response Prepare(Request req) {
        // your code here
        mutex.lock();
        try {
            int n = req.proposalNumber;
            if (instanceMap.get(req.seq) == null) {
                instanceMap.put(req.seq, new Instance(-1,-1, null));
            }
            Instance curIns = instanceMap.get(req.seq);
            highestDoneSeq[req.me] = req.highestDone;
            if (n > curIns.minProposal) {
                curIns.minProposal = n;
                return new Response(true, n, curIns.highestAcceptedProposal, curIns.value);
            } else {
                return new Response(false);
            }
        } finally {
            mutex.unlock();
        }
    }

    /**
     * Acceptor handle the accept request and give respond to the server
     *
     * @param req req(seq, proposalNumber, valueAccepted)
     * @return respond to the accept request
     */
    public Response Accept(Request req) {
        // your code here
        mutex.lock();
        try {
            int n = req.proposalNumber;
            Instance curIns = instanceMap.get(req.seq);
            instanceMap.put(req.seq, curIns);
            highestDoneSeq[req.me] = req.highestDone;
            if (n >= curIns.minProposal) {
                curIns.minProposal = n;
                curIns.highestAcceptedProposal = n;
                curIns.value = req.value;
                return new Response(true, n, n, req.value);
            } else {
                return new Response(false);
            }
        } finally {
            mutex.unlock();
        }
    }

    /**
     * Server sends Decide request to all the acceptors. Acceptors need to
     * change their value to the decided value.
     *
     * @param req req(seq, proposalNumber, valueAccepted)
     * @return respond to the decide request
     */
    public Response Decide(Request req) {
        // your code here
        mutex.lock();
        try {
            highestDoneSeq[req.me] = req.highestDone;
            Instance curIns = instanceMap.get(req.seq);
            curIns.value = req.value;
            curIns.state = State.Decided;
            return new Response(true);
        } finally {
            mutex.unlock();
        }
    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
        mutex.lock();
        try {
            if (highestDoneSeq[me] <= seq) {
                highestDoneSeq[me] = seq;
            }
        } finally {
            mutex.unlock();
        }
    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        // Your code here
        mutex.lock();
        try {
            if (instanceMap.isEmpty()) {
                return -1;
            }
            int maxSeq = Integer.MIN_VALUE;
            for (int seq : instanceMap.keySet()) {
                maxSeq = Math.max(maxSeq, seq);
            }
            return maxSeq;
        } finally {
            mutex.unlock();
        }
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().

     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.

     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.

     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min(){
        // Your code here
        mutex.lock();
        try {
            int minDoneValue = Integer.MAX_VALUE;
            for (int done : highestDoneSeq) {
                minDoneValue = Math.min(minDoneValue, done);
            }
            Iterator<Map.Entry<Integer, Instance>> iterator = instanceMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, Instance> entry = iterator.next();
                if (entry.getKey() < minDoneValue) {
                    iterator.remove();
                }
            }
            return minDoneValue + 1;
        } finally {
            mutex.unlock();
        }
    }


    /**
     * the application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. Status()
     * should just inspect the local peer state;
     * it should not contact other Paxos peers.
     */
    public retStatus Status(int seq){
        // Your code here
        mutex.lock();
        try {
            if (seq < Min()) {
                return new retStatus(State.Forgotten, null);
            }
            if (!instanceMap.containsKey(seq)) {
                return new retStatus(State.Pending, null);
            }
            Instance curIns = instanceMap.get(seq);
            return new retStatus(curIns.state, curIns.value);
        } finally {
            mutex.unlock();
        }
    }

    /**
     * helper class for Status() return
     */
    public class retStatus{
        public State state;
        public Object v;

        public retStatus(State state, Object v){
            this.state = state;
            this.v = v;
        }
    }

    public int getInstanceNum(){
        return this.instanceMap.size();
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill(){
        this.dead.getAndSet(true);
        if(this.registry != null){
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch(Exception e){
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead(){
        return this.dead.get();
    }

    public void setUnreliable(){
        this.unreliable.getAndSet(true);
    }

    public boolean isunreliable(){
        return this.unreliable.get();
    }


}
