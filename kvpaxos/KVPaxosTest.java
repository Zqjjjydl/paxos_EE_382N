package kvpaxos;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * This is a subset of entire test cases
 * For your reference only.
 */
public class KVPaxosTest {


    public void check(Client ck, String key, Integer value){
        Integer v = ck.Get(key);
        assertTrue("Get(" + key + ")->" + v + ", expected " + value, v.equals(value));
    }

    @Test
    public void TestBasic(){
        final int npaxos = 5;
        String host = "127.0.0.1";
        String[] peers = new String[npaxos];
        int[] ports = new int[npaxos];

        Server[] kva = new Server[npaxos];
        for(int i = 0 ; i < npaxos; i++){
            ports[i] = 1100+i;
            peers[i] = host;
        }
        for(int i = 0; i < npaxos; i++){
            kva[i] = new Server(peers, ports, i);
        }

        Client ck = new Client(peers, ports);
        System.out.println("Test: Basic put/get ...");
        ck.Put("app", 6);
        check(ck, "app", 6);
        ck.Put("a", 70);
        check(ck, "a", 70);

        System.out.println("... Passed");

    }
    @Test
    public void TestAdvanced(){
        final int npaxos = 5;
        String host = "127.0.0.1";
        String[] peers = new String[npaxos];
        int[] ports = new int[npaxos];

        Server[] kva = new Server[npaxos];
        for(int i = 0 ; i < npaxos; i++){
            ports[i] = 1100+i;
            peers[i] = host;
        }
        for(int i = 0; i < npaxos; i++){
            kva[i] = new Server(peers, ports, i);
        }

        Client ck1 = new Client(peers, ports);
        Client ck2 = new Client(peers, ports);
        System.out.println("Test: multiple client ...");
        ck1.Put("app", 6);
        check(ck2, "app", 6);
        ck2.Put("a", 70);
        check(ck1, "a", 70);

        System.out.println("Test: deaf server ...");
        kva[1].ports[0]= 1;
        kva[1].ports[1]= 1;
        ck1.Put("b", 9);
        check(ck2, "b", 9);
        ck2.Put("bbb", 79);
        check(ck1, "bbb", 79);

        System.out.println("Test: deaf server recover ...");
        kva[1].ports[1]= 1101;
        ck1.Put("c", 9);
        check(ck2, "c", 9);

        System.out.println("... Passed");

    }
}
