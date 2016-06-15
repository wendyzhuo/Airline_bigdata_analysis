
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Zhuang Zhuo <zhuo.z@husky.neu.edu>
 */
public class CompositeKey_mc implements WritableComparable<CompositeKey_mc>{
    
     private int dayOfWeek ;
     private int dest ;

    public CompositeKey_mc() {
    }

    public CompositeKey_mc(int week, int dest) {
        this.dayOfWeek = week;
        this.dest = dest;
    }

    @Override
    public String toString() {
        return (new StringBuilder()).append(dayOfWeek).append(", ").append(dest).toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dayOfWeek = in.readInt();
        dest = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(dayOfWeek);
        out.writeInt(dest);
    }

    
    
    
    @Override
    public int compareTo(CompositeKey_mc o) {
        int result =  -(new Integer(dayOfWeek)).compareTo(o.dayOfWeek);
        if (0 == result) {
            result =-(new Integer(dest)).compareTo(o.dest);
        }
        return result;
    }

    public int getDayOfWeek() {
        return dayOfWeek;
    }

    public void setDayOfWeek(int dayOfWeek) {
        this.dayOfWeek = dayOfWeek;
    }

    public int getDest() {
        return dest;
    }

    public void setDest(int dest) {
        this.dest = dest;
    }
    
    
    
    
}
