package datamapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.UUID;
import java.io.File;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import datamapreduce.DataFile;
import datamapreduce.FilePathInputFormat;

public class DataMapReduce extends Configured implements Tool {

	public static class DataMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("STARTING WITH MAPPER");
			
			String inputPath = value.toString();
			FileSystem fs = FileSystem.get(context.getConfiguration());


			processPath(new Path(inputPath), fs, context);
		}

		private void processPath(Path path, FileSystem fs, Context context) throws IOException, InterruptedException {
			if (fs.isDirectory(path)) {
				System.out.println("IS A DIRECTORY");
				FileStatus[] statuses = fs.listStatus(path);
				for (FileStatus status : statuses) {
					processPath(status.getPath(), fs, context);
				}
			} else {
				System.out.println("IS A FILE");
				StringBuilder fileContent = new StringBuilder();
				System.out.println("GOT FILE: " + path);
				fs.open(path);
				System.out.println("OPENED FILE: " + path);
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
					String line;
					System.out.println("DID STUFF WITH FILE");
					while ((line = reader.readLine()) != null) {
						fileContent.append(line).append("\n");
					}
				}
				System.out.println("DONE DOING STUFF WITH FILE");

				System.out.println("OUTPUTING HERE: " + path.toString().replace("dataset", "DataReducedOutput"));

				DataFile file = new DataFile(fileContent.toString());
				context.write(new Text(path.toString()), new Text(file.GetCleanedText()));
			}
		}
	}

	public static class DataReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			System.out.println("STARTED REDUCING");

			String filePath = key.toString();
			String outputPath = filePath;
			outputPath = outputPath.replace("dataset", "DataReducedOutput");
			

			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path file = new Path(outputPath);

			try (OutputStream os = fs.create(file);
				BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
				for (Text value : values) {
					writer.write(value.toString());
					writer.newLine();
				}
			}


			// StringBuilder content = new StringBuilder();

			// for (Text value : values) {
			// 	content.append(value.toString());
			// }

			// File outputFile = new File(outputPath);
			// outputFile.getParentFile().mkdirs();

			// try (FileWriter writer = new FileWriter(outputFile)) {
			// 	writer.write(content.toString());
			// }

			System.out.println("File written successfully to: " + outputPath);
			context.write(new Text(outputPath), new Text("File written successfully."));
		}
	}

	public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
		conf.set("outputPath", outputDir);

		Job job = Job.getInstance(conf, "datamapreduce");

		job.setInputFormatClass(FilePathInputFormat.class);
		job.setJarByClass(DataMapReduce.class);

		job.setMapperClass(DataMapper.class);
		job.setReducerClass(DataReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int resultBit = ToolRunner.run(new Configuration(), new DataMapReduce(), args);
		System.exit(resultBit);
	}


	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		if (runJob(conf, args[0], args[1]) != 0) 	 {
			return 1;
		}												
		return 0;
	}
}