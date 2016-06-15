/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package final_hbase;

/**
 *
 * @author Zhuang Zhuo <zhuo.z@husky.neu.edu>
 */
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author xuefanrong
 */
 public class Final_Hbase {
    public static void main(String[] args) throws IOException {

      Configuration con = HBaseConfiguration.create();
      HBaseAdmin admin = new HBaseAdmin(con);
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("2006a"));
      tableDescriptor.addFamily(new HColumnDescriptor("a_data"));
      admin.createTable(tableDescriptor);
      System.out.println(" Table created ");     
      Configuration config = HBaseConfiguration.create();
      HTable hTable = new HTable(config, "2006a");
 
              Path pt=new Path("hdfs://localhost:9000/data/2006a.csv");
 Configuration confi = new Configuration();

           FileSystem fs = FileSystem.get(confi);
                     BufferedReader b=new BufferedReader(new InputStreamReader(fs.open(pt)));
      String test = null;
      int count = 0;
      while((test = b.readLine())!=null){
          count++;
      }
       BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
      for(int i = 1;i<=count;i++){
          Put p = new Put(Bytes.toBytes("row"+i));   
      String s=null;        
      if((s=br.readLine())!=""){
          String[] insert = s.split(",");
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("Year"), Bytes.toBytes(insert[0]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("Month"), Bytes.toBytes(insert[1]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("DayofMonth"), Bytes.toBytes(insert[2]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("DayOfWeek"), Bytes.toBytes(insert[3]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("DepTime"), Bytes.toBytes(insert[4]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("CRSDepTime"), Bytes.toBytes(insert[5]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("ArrTime"), Bytes.toBytes(insert[6]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("CRSArrTime"), Bytes.toBytes(insert[7]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("UniqueCarrier"), Bytes.toBytes(insert[8]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("FlightNum"), Bytes.toBytes(insert[9]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("TailNum"), Bytes.toBytes(insert[10]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("ActualElapsedTime"), Bytes.toBytes(insert[11]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("CRSElapsedTime"), Bytes.toBytes(insert[12]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("Airtime"), Bytes.toBytes(insert[13]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("ArrDelay"), Bytes.toBytes(insert[14]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("DepDelay"), Bytes.toBytes(insert[15]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("Origin"), Bytes.toBytes(insert[16]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("Dest"), Bytes.toBytes(insert[17]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("Disntance"), Bytes.toBytes(insert[18]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("TaxiIn"), Bytes.toBytes(insert[19]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("TaxiOut"), Bytes.toBytes(insert[20]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("Cancelled"), Bytes.toBytes(insert[21]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("CancellationCode"), Bytes.toBytes(insert[22]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("Diverted"), Bytes.toBytes(insert[23]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("CarrierDelay"), Bytes.toBytes(insert[24]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("WeatherDelay"), Bytes.toBytes(insert[25]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("NASDelay"), Bytes.toBytes(insert[26]));
                p.add(Bytes.toBytes("a_data"), Bytes.toBytes("SecurityDelay"), Bytes.toBytes(insert[27]));
                p.add(Bytes.toBytes("a_data"),Bytes.toBytes("LateAircraftDelay"),Bytes.toBytes(insert[28]));
                // p.add(Bytes.toBytes("a_data"),Bytes.toBytes("LateAircraftDelay"),Bytes.toBytes(insert[29]));
          // Saving the put Instance to the HTable.
          hTable.put(p);
      }
      }
      
      // closing HTable
      hTable.close();
     
      }
    
}

