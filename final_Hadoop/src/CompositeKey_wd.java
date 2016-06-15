
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
public class CompositeKey_wd implements WritableComparable<CompositeKey_wd>{
    
     private String dayOfWeek ;
     private String dest ;

    public CompositeKey_wd() {
    }

    public CompositeKey_wd(String week, String dest) {
        this.dayOfWeek = week;
        this.dest = dest;
    }

    @Override
    public String toString() {
        return (new StringBuilder()).append(dayOfWeek).append(", ").append(dest).toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dayOfWeek = in.readUTF();
        dest = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(dayOfWeek);
        out.writeUTF(dest);
    }

    @Override
    public int compareTo(CompositeKey_wd o) {
        int result = dayOfWeek.compareTo(o.dayOfWeek);
        if (0 == result) {
            result = dest.compareTo(o.dest);
        }
        return result;
    }

    public String getDayOfWeek() {
        return dayOfWeek;
    }

    public void setDayOfWeek(String dayOfWeek) {
        this.dayOfWeek = dayOfWeek;
    }

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }
    
    
    
    
}