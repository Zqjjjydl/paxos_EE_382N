package kvpaxos;
import java.io.Serializable;

/**
 * You may find this class useful, free to use it.
 */
public class Op implements Serializable{
    static final long serialVersionUID=33L;
    String op;
    int ClientSeq;
    int opSeq;//operation sequence number
    String key;
    int value;

    public Op(String op,int ClientSeq, int opSeq, String key, int value){
        this.op = op;
        this.ClientSeq=ClientSeq;
        this.opSeq=opSeq;
        this.key = key;
        this.value = value;
    }
    @Override
    public boolean equals(Object obj){
        if(this==obj){
            return true;
        }
        if(obj instanceof Op){
            Op op2=(Op)obj;
            return this.op.equals(op2.op)&&this.ClientSeq==op2.ClientSeq
                    &&this.opSeq==op2.opSeq&&this.key.equals(op2.key)&&this.value==op2.value;
        }
        else{
            return false;
        }
    }
}
