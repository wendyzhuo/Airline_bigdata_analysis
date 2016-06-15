
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
public class MapReduce3 {

   /**
     * @param args the command line arguments
     */
   
    static class TempMapper extends  Mapper<LongWritable, Text, CompositeKey_wd, IntWritable> {
        CompositeKey_wd wd = new CompositeKey_wd();
        
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // 打印样本: Before Mapper: 0, 2000010115
//System.out.println("Before Mapper: " + key + "." + value);
            String line = value.toString();

            try {
                String[] lineSplit = line.split(",");
                // String requestUrl = line.substring(0, 10);
                 wd.setDayOfWeek(lineSplit[1]);
               wd.setDest(lineSplit[22]);
                String requestUrl = lineSplit[21];
               if(requestUrl.equals("1")){
                   context.write(wd, new IntWritable(1));
               }
              //  System.out.println("" + "After Mapper:" + new Text(requestUrl) + "," + new IntWritable(1));
                
            } // context.write(out,one);  
            catch (java.lang.ArrayIndexOutOfBoundsException e) {
                //  context.getCounter(Counter.LINESKIP).increment(1); 

            }

        }

    }
    

    static class TempReducer extends Reducer<CompositeKey_wd, IntWritable, CompositeKey_wd, IntWritable> {
        @Override
        public void reduce(CompositeKey_wd key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {

            // System.out.println("Before Reduce:" + key + ",");
            int count = 0;
            for (IntWritable v : values) {
                count = count + v.get();
            }
            try {
                context.write(key, new IntWritable(count));
             //   System.out.println( ""+ "After Reduce:" + key + "," + count);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
    
    
    static class TempMapper2 extends  Mapper<LongWritable, Text, IntWritable, CompositeKey_wd> {
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
               
                context.write(new IntWritable(val),wd);
            } // context.write(out,one);  
            catch (java.lang.ArrayIndexOutOfBoundsException e) {
                //  context.getCounter(Counter.LINESKIP).increment(1); 

            }
        
        }
      
       
                
    }
    
     
    static class TempReduce2 extends  Reducer<IntWritable, CompositeKey_wd,Text, IntWritable> {
       ArrayList<LinkedHashMap<String,Integer>> tm = new ArrayList<LinkedHashMap<String,Integer>>();
      
       int count=0;

        @Override
        protected void reduce(IntWritable key,Iterable<CompositeKey_wd> values, Context context) throws IOException, InterruptedException {
         for(CompositeKey_wd v :values){
         int a = Integer.parseInt(v.getDayOfWeek())-1;
       if(tm.size()<7){
            for(int i=0;i<12; i++){
                
             LinkedHashMap<String,Integer> f  = new LinkedHashMap<>();
             tm.add(f);
         }
           
       }
         LinkedHashMap<String,Integer> fin = tm.get(a);
             fin.put(v.toString(), key.get());
         
         
         
         System.out.println("" + "reduce2:" + a + " || " +v.toString() + "| "+ fin.size()+" |"+key.get());
       // context.write(result, key);
         }     
        }
      
      
      
        @Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
                    
                    for(LinkedHashMap<String,Integer> f: tm){
                        int i=0;
                        for (Map.Entry<String,Integer> entry : f.entrySet()) {
                            i++;
                            if(i==f.size()){
                                Text fi = new Text(entry.getKey());
                                 context.write(fi,new IntWritable(entry.getValue()));  
                            }
                        }
                    }
                    
      
                }
    }
   
    
 
    
    
    
    public static void main(String[] args) throws Exception {

        //输入路径
        String dst = "hdfs://localhost:9000/data/2006a.csv";

        //输出路径，必须是不存在的，空文件加也不行。
      //  String dstOut = "hdfs://localhost:9000/mapreduce/result3/1";
         String dstOut = "/Users/wendyzhuo/NetBeansProjects/final_Hadoop/src/output3/1";
        String outFiles = "/Users/wendyzhuo/NetBeansProjects/final_Hadoop/src/output3/2";
        Configuration hadoopConfig = new Configuration();

        hadoopConfig.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );

        hadoopConfig.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );

        Job job = new Job(hadoopConfig);
        Job job2 = new Job(hadoopConfig);
 
        FileInputFormat.addInputPath(job, new Path(dst));
        FileOutputFormat.setOutputPath(job, new Path(dstOut));
        FileInputFormat.addInputPath(job2, new Path(dstOut));
        FileOutputFormat.setOutputPath(job2, new Path(outFiles));
        
        JobConf map1Conf = new JobConf(false);
        ChainMapper.addMapper(job,TempMapper.class,LongWritable.class,Text.class,CompositeKey_wd.class,IntWritable.class,map1Conf);
        JobConf reduceConf = new JobConf(false);
         ChainReducer.setReducer(job,TempReducer.class,CompositeKey_wd.class,IntWritable.class,CompositeKey_wd.class,IntWritable.class,reduceConf);        
        
         JobConf map2Conf = new JobConf(false);
        ChainMapper.addMapper(job2,TempMapper2.class,LongWritable.class,Text.class,IntWritable.class,CompositeKey_wd.class,map2Conf);
       JobConf map3Conf = new JobConf(false);
        ChainReducer.setReducer(job2,TempReduce2.class,IntWritable.class,CompositeKey_wd.class,Text.class,IntWritable.class,map3Conf);
//       
      //  JobClient.runJob(job);
        
        //指定自定义的Mapper和Reducer作为两个阶段的任务处理类
//        job.setMapperClass(TempMapper.class);
//
//        job.setReducerClass(TempReducer.class);

        //设置最后输出结果的Key和Value的类型
        job.setOutputKeyClass(CompositeKey_wd.class);

        job.setOutputValueClass(IntWritable.class);
    
        job2.setMapOutputKeyClass(IntWritable.class);
     job2.setMapOutputValueClass(CompositeKey_wd.class);
    
   
        
  //  job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
 
        //执行job，直到完成
        job.waitForCompletion(true);
        System.out.println("Finished1");
    job2.waitForCompletion(true);
        System.out.println("Finished2");

    }

}
