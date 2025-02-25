package datamapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class FilePathRecordReader extends RecordReader<LongWritable, Text> {
    private boolean processed = false;
    private FileSplit fileSplit;
    private LongWritable key = new LongWritable();
    private Text value = new Text();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (!processed) {
            Path filePath = fileSplit.getPath();
            key.set(0); // Dummy key
            value.set(filePath.toString());
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        // Nothing to close
    }
}
