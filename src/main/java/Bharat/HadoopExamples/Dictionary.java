package Bharat.HadoopExamples;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class Dictionary
{
    public static class WordMapper extends Mapper<Text, Text, Text, Text>
    {
        private Text word = new Text();
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString(),",");
            while (itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                context.write(key, word);
            }
        }
    }
    public static class AllTranslationsReducer
    extends Reducer<Text,Text,Text,Text>
    {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values,
        Context context
        ) throws IOException, InterruptedException
        {
            String translations = "";
            for (Text val : values)
            {
                translations += "|"+val.toString();
            }
            result.set(translations);
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "dictionary");
       // job.setJarByClass(Dictionary.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(AllTranslationsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
/*
 *
 * Feb 10, 2014 2:28:49 AM org.apache.hadoop.util.NativeCodeLoader <clinit>
WARNING: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Feb 10, 2014 2:28:49 AM org.apache.hadoop.mapred.JobClient copyAndConfigureFiles
WARNING: Use GenericOptionsParser for parsing the arguments. Applications should implement Tool for the same.
Feb 10, 2014 2:28:49 AM org.apache.hadoop.mapred.JobClient copyAndConfigureFiles
WARNING: No job jar file set.  User classes may not be found. See JobConf(Class) or JobConf#setJar(String).
Feb 10, 2014 2:28:50 AM org.apache.hadoop.mapreduce.lib.input.FileInputFormat listStatus
INFO: Total input paths to process : 1
Feb 10, 2014 2:28:50 AM org.apache.hadoop.io.compress.snappy.LoadSnappy <clinit>
WARNING: Snappy native library not loaded
Feb 10, 2014 2:28:51 AM org.apache.hadoop.mapred.JobClient monitorAndPrintJob
INFO: Running job: job_local1730151659_0001
Feb 10, 2014 2:28:51 AM org.apache.hadoop.mapred.LocalJobRunner$Job run
INFO: Waiting for map tasks
Feb 10, 2014 2:28:51 AM org.apache.hadoop.mapred.LocalJobRunner$Job$MapTaskRunnable run
INFO: Starting task: attempt_local1730151659_0001_m_000000_0
Feb 10, 2014 2:28:52 AM org.apache.hadoop.util.ProcessTree isSetsidSupported
INFO: setsid exited with exit code 0
Feb 10, 2014 2:28:52 AM org.apache.hadoop.mapred.Task initialize
INFO:  Using ResourceCalculatorPlugin : org.apache.hadoop.util.LinuxResourceCalculatorPlugin@730c7f9a
Feb 10, 2014 2:28:52 AM org.apache.hadoop.mapred.MapTask runNewMapper
INFO: Processing split: file:/home/savanibharat/Documents/workspace-sts-3.4.0.RELEASE/HadoopExamples/input.txt:0+469941
Feb 10, 2014 2:28:52 AM org.apache.hadoop.mapred.MapTask$MapOutputBuffer <init>
INFO: io.sort.mb = 100
Feb 10, 2014 2:28:52 AM org.apache.hadoop.mapred.JobClient monitorAndPrintJob
INFO:  map 0% reduce 0%
Feb 10, 2014 2:28:53 AM org.apache.hadoop.mapred.MapTask$MapOutputBuffer <init>
INFO: data buffer = 79691776/99614720
Feb 10, 2014 2:28:53 AM org.apache.hadoop.mapred.MapTask$MapOutputBuffer <init>
INFO: record buffer = 262144/327680
Feb 10, 2014 2:28:56 AM org.apache.hadoop.mapred.MapTask$MapOutputBuffer flush
INFO: Starting flush of map output
Feb 10, 2014 2:28:56 AM org.apache.hadoop.mapred.MapTask$MapOutputBuffer sortAndSpill
INFO: Finished spill 0
Feb 10, 2014 2:28:56 AM org.apache.hadoop.mapred.Task done
INFO: Task:attempt_local1730151659_0001_m_000000_0 is done. And is in the process of commiting
Feb 10, 2014 2:28:56 AM org.apache.hadoop.mapred.LocalJobRunner$Job statusUpdate
INFO: 
Feb 10, 2014 2:28:56 AM org.apache.hadoop.mapred.Task sendDone
INFO: Task 'attempt_local1730151659_0001_m_000000_0' done.
Feb 10, 2014 2:28:56 AM org.apache.hadoop.mapred.LocalJobRunner$Job$MapTaskRunnable run
INFO: Finishing task: attempt_local1730151659_0001_m_000000_0
Feb 10, 2014 2:28:56 AM org.apache.hadoop.mapred.LocalJobRunner$Job run
INFO: Map task executor complete.
Feb 10, 2014 2:28:56 AM org.apache.hadoop.mapred.Task initialize
INFO:  Using ResourceCalculatorPlugin : org.apache.hadoop.util.LinuxResourceCalculatorPlugin@5be4e5c6
Feb 10, 2014 2:28:56 AM org.apache.hadoop.mapred.LocalJobRunner$Job statusUpdate
INFO: 
Feb 10, 2014 2:28:56 AM org.apache.hadoop.mapred.Merger$MergeQueue merge
INFO: Merging 1 sorted segments
Feb 10, 2014 2:28:57 AM org.apache.hadoop.mapred.Merger$MergeQueue merge
INFO: Down to the last merge-pass, with 1 segments left of total size: 524410 bytes
Feb 10, 2014 2:28:57 AM org.apache.hadoop.mapred.LocalJobRunner$Job statusUpdate
INFO: 
Feb 10, 2014 2:28:57 AM org.apache.hadoop.mapred.JobClient monitorAndPrintJob
INFO:  map 100% reduce 0%
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Task done
INFO: Task:attempt_local1730151659_0001_r_000000_0 is done. And is in the process of commiting
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.LocalJobRunner$Job statusUpdate
INFO: 
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Task commit
INFO: Task attempt_local1730151659_0001_r_000000_0 is allowed to commit now
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter commitTask
INFO: Saved output of task 'attempt_local1730151659_0001_r_000000_0' to output1.txt
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.LocalJobRunner$Job statusUpdate
INFO: reduce > reduce
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Task sendDone
INFO: Task 'attempt_local1730151659_0001_r_000000_0' done.
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.JobClient monitorAndPrintJob
INFO:  map 100% reduce 100%
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.JobClient monitorAndPrintJob
INFO: Job complete: job_local1730151659_0001
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO: Counters: 20
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:   File Output Format Counters 
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Bytes Written=423039
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:   File Input Format Counters 
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Bytes Read=469941
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:   FileSystemCounters
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     FILE_BYTES_READ=1464712
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     FILE_BYTES_WRITTEN=1573753
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:   Map-Reduce Framework
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Reduce input groups=11820
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Map output materialized bytes=524414
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Combine output records=0
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Map input records=20487
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Reduce shuffle bytes=0
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Physical memory (bytes) snapshot=0
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Reduce output records=11820
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Spilled Records=43234
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Map output bytes=481174
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Total committed heap usage (bytes)=324009984
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     CPU time spent (ms)=0
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Virtual memory (bytes) snapshot=0
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     SPLIT_RAW_BYTES=151
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Map output records=21617
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Combine input records=0
Feb 10, 2014 2:28:58 AM org.apache.hadoop.mapred.Counters log
INFO:     Reduce input records=21617

 * */
 