package diamondmapreduce;

/**
 *
 * @author yujia1986
 */
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Shell;

public class DiamondMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

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
//        String query = conf.get(DiamondMapReduce.QUERY);
//        String output = conf.get(DiamondMapReduce.OUTPUT);
        String database = conf.get(DiamondMapReduce.DATABASE);

        String[] execCommand = new String[4];
        execCommand[0] = "/vol/sge-tmp/diamondCommand.sh";
//        execCommand[0] = "/vol/sge-tmp/check.sh";
//        execCommand[0] = "./test.sh";
        execCommand[1] = database;
        execCommand[2] = key.toString();
//        execCommand[1] = key.toString();
//        execCommand[3] = ">" + value.toString();
        execCommand[3] = value.toString();
//        execCommand[2] = value.toString();

        //Create the external process
        Shell.ShellCommandExecutor p = new Shell.ShellCommandExecutor(execCommand);

//        context.write(NullWritable.get(), new Text(execCommand[0]+execCommand[1]+execCommand[2]+execCommand[3]));
//        if (value.getLength() != 0) {
            p.execute();
//        }

    }
}
