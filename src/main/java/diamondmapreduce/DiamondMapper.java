package diamondmapreduce;

/**
 *
 * @author yujia1986
 */
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Shell;

public class DiamondMapper extends Mapper<LongWritable, Text, Text, Text> {

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
        String database = conf.get(DiamondMapReduce.DATABASE);

        FileWriter file = new FileWriter("/vol/sge-tmp/yujia/input/" + key.toString());
        try (BufferedWriter bf = new BufferedWriter(file)) {
            bf.write(value.toString());
            bf.close();
        }
//        file.write(value.toString());
        file.close();

        File keyValuefile = new File("/vol/sge-tmp/yujia/input/" + key.toString());
        keyValuefile.setExecutable(true, false);
        keyValuefile.setReadable(true, false);
        keyValuefile.setWritable(true, false);

//use shellcommandexecutor to run DIAMOND
//        String[] execCommand = new String[3];
//        execCommand[0] = "/vol/sge-tmp/diamondAlignment.sh";
//        execCommand[1] = database;
//        execCommand[2] = key.toString();
//
//        //Create the external process
//        Shell.ShellCommandExecutor exec = new Shell.ShellCommandExecutor(execCommand);
//        int exitCode = 1;
//        exec.execute();
//
//        while (exitCode != 0) {
//            exitCode = exec.getExitCode();
//        }
//        
//use Runtime to run DIAMOND
        String command[] = {"/vol/biotools/bin/diamond", "blastp", "-q", "/vol/sge-tmp/yujia/input/" + key.toString(), "-d", database, "-a", "/vol/sge-tmp/yujia/tmp/" + key.toString(), "--seg", "no", "--sensitive", "-k", "30000", "-e", "0.00001"};
        Process p = Runtime.getRuntime().exec(command);
        p.waitFor();

        File tmpFile = new File("/vol/sge-tmp/yujia/tmp/" + key.toString() + ".daa");
        tmpFile.setExecutable(true, false);
        tmpFile.setReadable(true, false);
        tmpFile.setWritable(true, false);

        context.write(new Text("keys"), new Text(key.toString()));
    }
}
