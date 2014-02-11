package Bharat.HadoopExamples;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * @author savanibharat
 * 
 */
public class WordCounter {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {

				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			int sum = 0;
			while (values.hasNext()) {

				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		JobConf conf = new JobConf(WordCounter.class);
		conf.setJobName("WordCount");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);

	}
}
/*
Input
-------------------------------------------
hello world
hello to the world
hello hello hello world
world world world
hello
world
hi there
-------------------------------------------
Output
-------------------------------------------
hello	6
hi	1
the	1
there	1
to	1
world	7
-------------------------------------------
Console
-------------------------------------------
14/02/11 03:41:54 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
14/02/11 03:41:56 WARN conf.Configuration: session.id is deprecated. Instead, use dfs.metrics.session-id
14/02/11 03:41:56 INFO jvm.JvmMetrics: Initializing JVM Metrics with processName=JobTracker, sessionId=
14/02/11 03:41:56 INFO jvm.JvmMetrics: Cannot initialize JVM Metrics with processName=JobTracker, sessionId= - already initialized
14/02/11 03:41:57 WARN mapreduce.JobSubmitter: Use GenericOptionsParser for parsing the arguments. Applications should implement Tool for the same.
14/02/11 03:41:57 WARN mapreduce.JobSubmitter: No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
14/02/11 03:41:57 INFO mapred.FileInputFormat: Total input paths to process : 1
14/02/11 03:41:57 INFO mapreduce.JobSubmitter: number of splits:1
14/02/11 03:41:58 WARN conf.Configuration: mapred.output.value.class is deprecated. Instead, use mapreduce.job.output.value.class
14/02/11 03:41:58 WARN conf.Configuration: mapred.job.name is deprecated. Instead, use mapreduce.job.name
14/02/11 03:41:58 WARN conf.Configuration: mapred.input.dir is deprecated. Instead, use mapreduce.input.fileinputformat.inputdir
14/02/11 03:41:58 WARN conf.Configuration: mapred.output.dir is deprecated. Instead, use mapreduce.output.fileoutputformat.outputdir
14/02/11 03:41:58 WARN conf.Configuration: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
14/02/11 03:41:58 WARN conf.Configuration: mapred.output.key.class is deprecated. Instead, use mapreduce.job.output.key.class
14/02/11 03:41:58 WARN conf.Configuration: mapred.working.dir is deprecated. Instead, use mapreduce.job.working.dir
14/02/11 03:41:58 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_local791984119_0001
14/02/11 03:42:00 INFO mapreduce.Job: The url to track the job: http://localhost:8080/
14/02/11 03:42:00 INFO mapred.LocalJobRunner: OutputCommitter set in config null
14/02/11 03:42:00 INFO mapreduce.Job: Running job: job_local791984119_0001
14/02/11 03:42:00 INFO mapred.LocalJobRunner: OutputCommitter is org.apache.hadoop.mapred.FileOutputCommitter
14/02/11 03:42:00 INFO mapred.LocalJobRunner: Waiting for map tasks
14/02/11 03:42:00 INFO mapred.LocalJobRunner: Starting task: attempt_local791984119_0001_m_000000_0
14/02/11 03:42:01 INFO mapred.Task:  Using ResourceCalculatorProcessTree : [ ]
14/02/11 03:42:01 INFO mapred.MapTask: Processing split: file:/home/cloudera/Desktop/Hadoop Input and output/Word count assignment 1/hello world:0+94
14/02/11 03:42:01 INFO mapred.MapTask: numReduceTasks: 1
14/02/11 03:42:01 INFO mapred.MapTask: Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
14/02/11 03:42:08 INFO mapreduce.Job: Job job_local791984119_0001 running in uber mode : false
14/02/11 03:42:08 INFO mapred.MapTask: (EQUATOR) 0 kvi 26214396(104857584)
14/02/11 03:42:08 INFO mapred.MapTask: mapreduce.task.io.sort.mb: 100
14/02/11 03:42:08 INFO mapred.MapTask: soft limit at 83886080
14/02/11 03:42:08 INFO mapred.MapTask: bufstart = 0; bufvoid = 104857600
14/02/11 03:42:08 INFO mapred.MapTask: kvstart = 26214396; length = 6553600
14/02/11 03:42:09 INFO mapreduce.Job:  map 0% reduce 0%
14/02/11 03:42:09 INFO mapred.LocalJobRunner: 
14/02/11 03:42:09 INFO mapred.MapTask: Starting flush of map output
14/02/11 03:42:09 INFO mapred.MapTask: Spilling map output
14/02/11 03:42:09 INFO mapred.MapTask: bufstart = 0; bufend = 162; bufvoid = 104857600
14/02/11 03:42:09 INFO mapred.MapTask: kvstart = 26214396(104857584); kvend = 26214332(104857328); length = 65/6553600
14/02/11 03:42:09 INFO mapred.MapTask: Finished spill 0
14/02/11 03:42:09 INFO mapred.Task: Task:attempt_local791984119_0001_m_000000_0 is done. And is in the process of committing
14/02/11 03:42:09 INFO mapred.LocalJobRunner: file:/home/cloudera/Desktop/Hadoop Input and output/Word count assignment 1/hello world:0+94
14/02/11 03:42:09 INFO mapred.Task: Task 'attempt_local791984119_0001_m_000000_0' done.
14/02/11 03:42:09 INFO mapred.LocalJobRunner: Finishing task: attempt_local791984119_0001_m_000000_0
14/02/11 03:42:09 INFO mapred.LocalJobRunner: Map task executor complete.
14/02/11 03:42:09 INFO mapred.Task:  Using ResourceCalculatorProcessTree : [ ]
14/02/11 03:42:09 INFO mapred.Merger: Merging 1 sorted segments
14/02/11 03:42:10 INFO mapreduce.Job:  map 100% reduce 0%
14/02/11 03:42:10 INFO mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 58 bytes
14/02/11 03:42:10 INFO mapred.LocalJobRunner: 
14/02/11 03:42:10 INFO mapred.Task: Task:attempt_local791984119_0001_r_000000_0 is done. And is in the process of committing
14/02/11 03:42:10 INFO mapred.LocalJobRunner: 
14/02/11 03:42:10 INFO mapred.Task: Task attempt_local791984119_0001_r_000000_0 is allowed to commit now
14/02/11 03:42:10 INFO output.FileOutputCommitter: Saved output of task 'attempt_local791984119_0001_r_000000_0' to file:/home/cloudera/Desktop/Hadoop Input and output/Word count assignment 1/output/_temporary/0/task_local791984119_0001_r_000000
14/02/11 03:42:10 INFO mapred.LocalJobRunner: reduce > reduce
14/02/11 03:42:10 INFO mapred.Task: Task 'attempt_local791984119_0001_r_000000_0' done.
14/02/11 03:42:11 INFO mapreduce.Job:  map 100% reduce 100%
14/02/11 03:42:11 INFO mapreduce.Job: Job job_local791984119_0001 completed successfully
14/02/11 03:42:11 INFO mapreduce.Job: Counters: 27
	File System Counters
		FILE: Number of bytes read=644
		FILE: Number of bytes written=340076
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
	Map-Reduce Framework
		Map input records=7
		Map output records=17
		Map output bytes=162
		Map output materialized bytes=70
		Input split bytes=139
		Combine input records=17
		Combine output records=6
		Reduce input groups=6
		Reduce shuffle bytes=0
		Reduce input records=6
		Reduce output records=6
		Spilled Records=12
		Shuffled Maps =0
		Failed Shuffles=0
		Merged Map outputs=0
		GC time elapsed (ms)=159
		CPU time spent (ms)=0
		Physical memory (bytes) snapshot=0
		Virtual memory (bytes) snapshot=0
		Total committed heap usage (bytes)=331227136
	File Input Format Counters 
		Bytes Read=94
	File Output Format Counters 
		Bytes Written=52
-------------------------------------------
*/