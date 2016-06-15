
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Zhuang Zhuo <zhuo.z@husky.neu.edu>
 */
public class Mapreduce1 {

    /**
     * @param args the command line arguments
     */
    
    
     
    static class TempMapper extends Mapper<LongWritable, Text, CompositeKey_wd, IntWritable> {
        CompositeKey_wd wd = new CompositeKey_wd();
     @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
               //  String[] lineSplit = line.split(“ “);  
               String []words=value.toString().split(",");
               // requestUrl = requestUrl.substring(0,requestUrl.indexOf(‘ ‘)+1);  
                // Text out = new Text(requestUrl); 
              
               wd.setDayOfWeek(words[3]);
               wd.setDest(words[17]);
              //  System.out.println("After Mapper:"+ wd.getDayOfWeek() + "," + wd.getDest()+" | "+new IntWritable(1));
                context.write(wd, new IntWritable(1));
            
            } // context.write(out,one);  
            catch (java.lang.ArrayIndexOutOfBoundsException e) {
                //  context.getCounter(Counter.LINESKIP).increment(1); 
               
            }
          


        }

    }
static class TempReducer extends Reducer<CompositeKey_wd, IntWritable, CompositeKey_wd, IntWritable> {
        @Override
        public void reduce(CompositeKey_wd key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

           // System.out.println("Before Reduce:" + key + ",");
                int count = 0;
                for (IntWritable v : values) {
                    count = count + v.get();
                }
                try {
                   Text t = new Text(key.getDayOfWeek()+", "+key.getDest()); 
                    context.write(key, new IntWritable(count));
                 //   System.out.println(""+ "After Reduce:" + t + "|" + count);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
 
        }

    

  public static void main(String[] args) throws Exception {
        //输入路径
        String dst = "hdfs://localhost:9000/data/2006a.csv";
        //输出路径，必须是不存在的，空文件加也不行。
        String dstOut = "hdfs://localhost:9000/Number6";
        String outFiles = "/Users/wendyzhuo/NetBeansProjects/final_Hadoop/src/output";
        Configuration hadoopConfig = new Configuration();

        hadoopConfig.set("fs.hdfs.impl",  org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        hadoopConfig.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());

        Job job = new Job(hadoopConfig);

         
        //如果需要打成jar运行，需要下面这句
        //job.setJarByClass(NewMaxTemperature.class);
        //job执行作业时输入和输出文件的路径
        FileInputFormat.addInputPath(job, new Path(dst));
        //FileOutputFormat.setOutputPath(job, new Path(dstOut));
FileOutputFormat.setOutputPath(job, new Path(outFiles));
        //指定自定义的Mapper和Reducer作为两个阶段的任务处理类

        job.setMapperClass(TempMapper.class);

        job.setReducerClass(TempReducer.class);

         

        //设置最后输出结果的Key和Value的类型

        job.setOutputKeyClass(CompositeKey_wd.class);

        job.setOutputValueClass(IntWritable.class);

         

        //执行job，直到完成

        job.waitForCompletion(true);

        System.out.println("Finished");

    }

 
    
}
