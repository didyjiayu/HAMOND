package blastmapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Shell;

public class BlastMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

//    private String localDB;
//
//    @Override
//    public void setup(Context context) throws IOException {
//        Configuration conf = context.getConfiguration();
//        URI[] local = context.getCacheFiles();
//        this.localDB = local[0].getPath();
//    }
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        
        Configuration conf = context.getConfiguration();
//        String query = conf.get(BlastMapReduce.QUERY);
//        String output = conf.get(BlastMapReduce.OUTPUT);
        String database = conf.get(BlastMapReduce.DATABASE);
        
        String[] execCommand = new String[4];
        execCommand[0] = "/vol/sge-tmp/diamondCommand.sh";
        execCommand[1] = database;
        execCommand[2] = key.toString();
        execCommand[3] = ">" + value.toString();
        
        //Create the external process
        Shell.ShellCommandExecutor p= new Shell.ShellCommandExecutor(execCommand);
        
        if (value.getLength()!=0) {
            p.execute();
//            context.write(NullWritable.get(), new Text(this.localCommand));
        }

    }
}
