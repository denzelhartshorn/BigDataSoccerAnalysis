package datamapreduce;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilePathInputFormat extends FileInputFormat<LongWritable, Text> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false; // Do not split files; process them as single units
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        // Get all file paths recursively from the input directory
        List<InputSplit> splits = new ArrayList<>();
        Path[] inputPaths = FileInputFormat.getInputPaths(job);
        FileSystem fs = FileSystem.get(job.getConfiguration());

        for (Path inputPath : inputPaths) {
            FileStatus[] fileStatuses = fs.listStatus(inputPath);
            for (FileStatus status : fileStatuses) {
                // if (status.isDirectory()) {
                //     continue;
                // }
                splits.add(new org.apache.hadoop.mapreduce.lib.input.FileSplit(
                        status.getPath(), 0, status.getLen(), null));
            }
        }
        return splits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new FilePathRecordReader();
    }
}
