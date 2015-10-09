package blastmapreduce;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

// need to be modified
public class FileRecordReader extends RecordReader<String, String> {

    private Path path;
    private FileSystem fs;
    private boolean done = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        path = ((FileSplit) split).getPath();
        fs = path.getFileSystem(context.getConfiguration());
    }

    @Override
    public float getProgress() throws IOException {
        System.out.println("in getProgress : " + done);
        if (done) {
            return 1.0f;
        } else {
            return 0.0f;
        }
    }

    @Override
    public String getCurrentKey() throws IOException, InterruptedException {
        System.out.println("in current key " + path.toString() + " :" + done);
		// if (done){
        // return null;
        // }else{
        String pathName = path.getName();
        int index = pathName.lastIndexOf("/");
        return pathName.substring(index + 1, pathName.length());
        // }
    }

    @Override
    public String getCurrentValue() throws IOException, InterruptedException {
        System.out.println(" get Current Value " + path.toString() + " :" + done);
		// if (done){
        // return null;
        // }else{
        return path.toString();
        // }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        System.out.println("next keyvalue : " + path.toString() + " :" + done);
        if (done) {
            return false;
        } else {
            done = true;
            return true;
        }
    }

    @Override
    public void close() throws IOException {
        done = true;
    }
} // end of FileRecordReader
