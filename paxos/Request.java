package paxos;
import java.io.Serializable;

/**
 * Please fill in the data structure you use to represent the request message for each RMI call.
 * Hint: You may need the sequence number for each paxos instance and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 * Hint: Easier to make each variable public
 */
public class Request implements Serializable {
    static final long serialVersionUID=1L;
    // Your data here
    public int seq; // sequence number, which is used to indicate proposal
    public int proposalNumber;
    public Object value;
    public int me; // indicate which machine sent the request
    public int highestDone; // piggyback the Done value to help other peers to learn

    // Your constructor and methods here
    public Request(int seq, int proposalNumber, Object value, int me, int highestDone) {
        this.seq = seq;
        this.proposalNumber = proposalNumber;
        this.value = value;
        this.me = me;
        this.highestDone = highestDone;
    }
}
