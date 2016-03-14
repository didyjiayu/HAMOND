package diamondmapreduce;

/**
 *
 * @author yujia1986
 */
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;

/**
 *
 * @author yujia1986
 */
public class DiamondReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException,
            InterruptedException {

        Configuration conf = context.getConfiguration();
        String query = conf.get(DiamondMapReduce.QUERY);
        Path filePath = Paths.get(query);
        String fileName = filePath.getFileName().toString();

        String[] execCommand = new String[3];
        execCommand[0] = "/vol/sge-tmp/diamondView.sh";
        execCommand[1] = fileName;

        for (Text singleKey : value) {
            execCommand[2] = singleKey.toString();
            Shell.ShellCommandExecutor p = new Shell.ShellCommandExecutor(execCommand);
            try {
                p.execute();
                p.close();
            } catch (ExitCodeException e) {
                System.out.println("Skipped");
            }
        }
    }
    
//    public void reduce(Text key, Text value, Context context) throws IOException,
//            InterruptedException {
//
//        Configuration conf = context.getConfiguration();
//        String query = conf.get(DiamondMapReduce.QUERY);
//        Path filePath = Paths.get(query);
//        String fileName = filePath.getFileName().toString();
//
//        String[] execCommand = new String[3];
//        execCommand[0] = "/vol/sge-tmp/diamondView.sh";
//        execCommand[1] = fileName;
//        execCommand[2] = key.toString();
//        Shell.ShellCommandExecutor p = new Shell.ShellCommandExecutor(execCommand);
//        p.execute();
////        context.write(key, value);
//
//    }

}
