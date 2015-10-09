package blastmapreduce;

import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class DataFileInputFormat extends FileInputFormat<String, String> {

    @Override
    public RecordReader<String, String> createRecordReader(
            InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

//			context.reporter.setStatus(split.toString());
        return new FileRecordReader();
    }
}
