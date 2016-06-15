
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Zhuang Zhuo <zhuo.z@husky.neu.edu>
 */
public class MapReduce4 {

    /**
     * @param args the command line arguments
     */
    static class TempMapper extends  Mapper<LongWritable, Text, CompositeKey_wd,IntWritable > {
        CompositeKey_wd wd = new CompositeKey_wd();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                  
            String line = value.toString();

            try {
                String[] lineSplit = line.split(",");
            
                 wd.setDayOfWeek(lineSplit[16]);
               wd.setDest(lineSplit[17]);
               int a = Integer.parseInt(lineSplit[14]);
               int b = Integer.parseInt(lineSplit[15]);
              int c=a+b;
              
                   context.write(wd,new IntWritable(c));
               //  System.out.println( ""+ "After map1:" + wd.toString() + "||" +a+" "+b+"="+ c);
               
            } 
            catch (java.lang.ArrayIndexOutOfBoundsException e) {
             
            }
        
      
    }
    

    static class TempReducer extends Reducer<CompositeKey_wd, IntWritable, CompositeKey_wd, Text> {

            @Override
            protected void reduce(CompositeKey_wd key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
             int count = 0;
              int count1 = 0;
            for (IntWritable v : values) {
                count = count + v.get();
                 count1++;
            }
            try {
                Text a = new Text(count + ","+count1);
            
                context.write(key,a);
             //   System.out.println( ""+ "After Reduce1:" + key.toString() + "||" + count+", "+count1);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            }
       
        
   

    }
    
      static class TempMapper2 extends  Mapper<LongWritable, Text, CompositeKey_wd, CompositeKey_mc> {
            CompositeKey_wd wd = new CompositeKey_wd();
            CompositeKey_mc mc = new CompositeKey_mc();
            @Override
          protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
               String line = value.toString();
 
            try {
                String[] lineSplit = line.split("\t");
                String requestUrl = lineSplit[0];
                String requestUrl1 = lineSplit[1];
                  String[] lineSplit2 = requestUrl.split(",");
                    wd.setDayOfWeek(lineSplit2[0]);
               wd.setDest(lineSplit2[1]);
                String[] lineSplit3 = requestUrl1.split(",");
                int a = Integer.parseInt(lineSplit3[0]);
               int b = Integer.parseInt(lineSplit3[1]);
                mc.setDayOfWeek(a);
                mc.setDest(b);
            //   System.out.println( ""+ "After Reduce:" + wd.toString() + "||" + mc.toString());

               
                context.write(wd,mc);
            } // context.write(out,one);  
            catch (java.lang.ArrayIndexOutOfBoundsException e) {
                //  context.getCounter(Counter.LINESKIP).increment(1); 

            }
            
            
            }
    
      
       
                
    }
    
     
    static class TempReduce2 extends  Reducer<CompositeKey_wd, CompositeKey_mc,CompositeKey_wd, IntWritable> {

            @Override
            protected void reduce(CompositeKey_wd key, Iterable<CompositeKey_mc> values, Context context) throws IOException, InterruptedException {
              int count = 0;
             
            for (CompositeKey_mc v : values) {
                count = v.getDayOfWeek()/v.getDest();
                
            }
            try {
              
             //  System.out.println( ""+ "After Reduce2:" + key.toString() + "," + count);
                context.write(key,new IntWritable(count));
             

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
                
                
            }
      
    }
    
    static class TempMapper3 extends  Mapper<LongWritable, Text, IntWritable, Text> {
            CompositeKey_wd wd = new CompositeKey_wd();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                  String line = value.toString();
 
            try {
                String[] lineSplit = line.split("\t");
                // String requestUrl = line.substring(0, 10);
                String requestUrl = lineSplit[0];
                  String[] lineSplit2 = requestUrl.split(",");
                    wd.setDayOfWeek(lineSplit2[0]);
               wd.setDest(lineSplit2[1]);
               
                 int val = Integer.parseInt(lineSplit[1]);
               //   System.out.println( ""+ "After Reduce2:" +val + "," + wd.toString());
                context.write(new IntWritable(val),new Text(wd.toString()));
            } // context.write(out,one);  
            catch (java.lang.ArrayIndexOutOfBoundsException e) {
                //  context.getCounter(Counter.LINESKIP).increment(1); 

            }
        
        } 
        
         
      
       
                
    }
    
     
       static class TempReduce3 extends  Reducer<IntWritable, Text, IntWritable,Text> {
       LinkedHashMap<String,Integer> tm = new LinkedHashMap<String,Integer>();
      int count=0;
        @Override
        public void reduce(IntWritable key,  Iterable<Text> values, Context context)
                throws IOException, InterruptedException {   
            
                  
                for (Text val : values) {
                    count++;
       String a = val.toString();
        int b =key.get();
         tm.put(a, b);
        //  System.out.println("" + "reduce3:" + a + " || " +tm.get(val));
       // context.write(result, key);
                }
       
        }
        @Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
                    ArrayList<LinkedHashMap<String,Integer>> a = new ArrayList();
           int i=0;
            for (Map.Entry<String,Integer> entry : tm.entrySet()) {
                i++;
                if(i>count-31){
                      LinkedHashMap<String,Integer> t = new LinkedHashMap<String,Integer>();
                      t.put(entry.getKey(), entry.getValue());
                     a.add(t);
                }
                
            }
            for(int j=a.size()-1;j>0;j--){
                      Text result = new Text();
             
                     for (Map.Entry<String,Integer> entry : a.get(j).entrySet()) {
                            result.set(entry.getKey());
                    context.write(new IntWritable(entry.getValue()),result);  
                }

        
       
        }
                }
    }
    
    public static void main(String[] args) throws Exception {

        //输入路径
        String dst = "hdfs://localhost:9000/data/2006a.csv";

        //输出路径，必须是不存在的，空文件加也不行。
      //  String dstOut = "hdfs://localhost:9000/mapreduce/result3/1";
         String dstOut = "/Users/wendyzhuo/NetBeansProjects/final_Hadoop/src/output4/1";
        String outFiles = "/Users/wendyzhuo/NetBeansProjects/final_Hadoop/src/output4/2";
        String outFiles2 = "/Users/wendyzhuo/NetBeansProjects/final_Hadoop/src/output4/3";
        Configuration hadoopConfig = new Configuration();

        hadoopConfig.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );

        hadoopConfig.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );

        Job job = new Job(hadoopConfig);
        Job job2 = new Job(hadoopConfig);
        Job job3 = new Job(hadoopConfig);
        
        FileInputFormat.addInputPath(job, new Path(dst));
        FileOutputFormat.setOutputPath(job, new Path(dstOut));
        FileInputFormat.addInputPath(job2, new Path(dstOut));
        FileOutputFormat.setOutputPath(job2, new Path(outFiles));
          FileInputFormat.addInputPath(job3, new Path(outFiles));
        FileOutputFormat.setOutputPath(job3, new Path(outFiles2));
        
        JobConf map1Conf = new JobConf(false);
        ChainMapper.addMapper(job,TempMapper.class,LongWritable.class,Text.class,CompositeKey_wd.class,IntWritable.class,map1Conf);
        JobConf reduceConf = new JobConf(false);
         ChainReducer.setReducer(job,TempReducer.class,CompositeKey_wd.class,IntWritable.class,CompositeKey_wd.class,IntWritable.class,reduceConf);        
        
         JobConf map2Conf = new JobConf(false);
        ChainMapper.addMapper(job2,TempMapper2.class,LongWritable.class,Text.class,CompositeKey_wd.class,CompositeKey_mc.class,map2Conf);
       JobConf map3Conf = new JobConf(false);
        ChainReducer.setReducer(job2,TempReduce2.class,CompositeKey_wd.class,CompositeKey_mc.class,CompositeKey_wd.class,IntWritable.class,map3Conf);

     JobConf maplConf = new JobConf(false);
        ChainMapper.addMapper(job3,TempMapper3.class,LongWritable.class,Text.class,IntWritable.class,Text.class,maplConf);
       JobConf mapllConf = new JobConf(false);
        ChainReducer.setReducer(job3,TempReduce3.class,IntWritable.class,Text.class,IntWritable.class,Text.class,mapllConf);



//       
      //  JobClient.runJob(job);
        
        //指定自定义的Mapper和Reducer作为两个阶段的任务处理类
//        job.setMapperClass(TempMapper.class);
//
//        job.setReducerClass(TempReducer.class);

        //设置最后输出结果的Key和Value的类型
        job.setOutputKeyClass(CompositeKey_wd.class);

        job.setOutputValueClass(IntWritable.class);
    
        job2.setMapOutputKeyClass(CompositeKey_wd.class);
     job2.setMapOutputValueClass(CompositeKey_mc.class);
    
    job3.setMapOutputKeyClass(IntWritable.class);
     job3.setMapOutputValueClass(Text.class);
        
  //  job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
 
        //执行job，直到完成
    //    job.waitForCompletion(true);
        System.out.println("Finished1");
  //  job2.waitForCompletion(true);
        System.out.println("Finished2");

        job3.waitForCompletion(true);
        System.out.println("Finished2");
    }
    
    
    }
}


