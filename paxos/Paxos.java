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
        int proposalNumber;
        Object value;
        State state; // status of the current proposal

        public Instance(int proposalNumber, Object value, State state) {
            this.proposalNumber = proposalNumber;
            this.value = value;
            state = State.Pending;
        }
    }

    Map<Integer, Instance> instanceMap; // agreements
    int peersNum; // number of peers
    int majority; // number of majority
    int curSeq; // current sequence
    Object curVal;
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
        curSeq = -1;
        curVal = null;
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
        mutex.lock();
        try {
            // reset req and v in Paxos object, and pass it to the new Thread
            this.curSeq = seq;
            this.curVal = value;
        } finally {
            mutex.unlock();
        }
        // start a new thread
        Thread thread = new Thread(this);
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
        while (Status(this.curSeq).state != State.Decided) {
            if (this.curSeq < Min()) {
                return;
            }
            /* ------------------ phase 1: Prepare ------------------ */
            // choose a unique and higher proposal number
            proposalNum = highestNumSeen + 1;
            highestNumSeen = proposalNum;
            // sent prepare(n) to all servers and get the Response
            Request[] requests = new Request[peers.length];
            Response[] responses = new Response[peers.length];
            for (int id = 0; id < peers.length; id++) {
                requests[id] = new Request(curSeq, proposalNum, null, id, highestDoneSeq[id]);
                // local peer: no need to send rmi call
                if (id == me) {
                    Prepare(requests[id]);
                } else {
                    responses[id] = Call("Prepare", requests[id], id);
                }
            }
            int ackCount = 0;
            int highestNumAccepted = Integer.MIN_VALUE;
            int highestId = -1;
            for (int id = 0; id < responses.length; id++) {
                Response response = responses[id];
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
            if (ackCount >= majority) {
                if (highestNumAccepted > proposalNum) {
                    sentValue = responses[highestId].valueAccepted;
                }
                /* ------------------ phase 2: Accept ------------------ */
                for (int id = 0; id < peers.length; id++) {
                    requests[id] = new Request(curSeq, proposalNum, sentValue, id, highestDoneSeq[id]);
                    if (id == me) {
                        Accept(requests[id]);
                    } else {
                        responses[id] = Call("Accept", requests[id], id);
                    }
                }
            }
            // phase 3: decide

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
            // 1. get the valueAccepted from the client's request
            // 2. broadcast the prepare proposal
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
            // 1. find whether there are old accepted prepare chosen
            //      1.1 chosen. accept proposal with chosen valueAccepted
            //      1.2 not chosen
            //         1.2.1 new proposer see it: use existing valueAccepted, all proposal success
            //         1.2.2 new proposer doesn't see it: new proposer chooses its own valueAccepted, older proposer blocked
            //
        } finally {
            mutex.unlock();
        }
    }

    /**
     * Server sends Decide request to all the acceptors. Acceptors need to
     * change their valueAccepted to the decided valueAccepted.
     *
     * @param req req(seq, proposalNumber, valueAccepted)
     * @return respond to the decide request
     */
    public Response Decide(Request req) {
        // your code here
        mutex.lock();
        try {

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
        if (highestDoneSeq[me] <= seq) {
            highestDoneSeq[me] = seq;
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
            if (instanceMap.containsKey(seq)) {
                throw new RuntimeException("No Instance Found");
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
