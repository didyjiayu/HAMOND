package diamondmapreduce;

/**
 *
 * @author yujia1986
 */
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.security.UserGroupInformation;
import sidefunctions.DeleteIntermediateFiles;

public class DiamondMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String localDB;
    private String diamond;

    //get local path of DIAMOND and database
    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        Path[] local = context.getLocalCacheFiles();
        this.diamond = local[0].toString();
        this.localDB = local[1].toString();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {

        //get query and database name from mapreduce driver
        Configuration conf = context.getConfiguration();
        String query = conf.get(DiamondMapReduce.QUERY);
        String dataBase = conf.get(DiamondMapReduce.DATABASE);

        //write key-value pair to local tmp
        FileWriter file = new FileWriter("/tmp/" + key.toString());
        try (BufferedWriter bf = new BufferedWriter(file)) {
            bf.write(value.toString());
            bf.close();
        }
//        file.write(value.toString());
        file.close();

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
        //use runtime to execute alignment, intermediate binary files are stored in local tmp
        String alignment[] = {this.diamond, "blastp", "-q", "/tmp/" + key.toString(), "-d", this.localDB, "-a", "/tmp/" + key.toString(), "--seg", "no", "--sensitive", "-k", "30000", "-e", "0.00001"};
        Process p1 = Runtime.getRuntime().exec(alignment);
        p1.waitFor();

        //view the binary files to tabular output file, view output will be streammized into HDFS
        String hadoopUser = UserGroupInformation.getCurrentUser().getUserName();
        String view[] = {this.diamond, "view", "-a", "/tmp/" + key.toString()};
        Process p2 = Runtime.getRuntime().exec(view);
        FileSystem fs = FileSystem.get(conf);
        //process stream copied to HDFS stream
        InputStream in = p2.getInputStream();
        FSDataOutputStream out = fs.append(new Path(hadoopUser + "/" + query + ".out"));
        IOUtils.copyBytes(in, out, 4096, true);
        p2.waitFor();
        
        //delete all intermediate files
        DeleteIntermediateFiles.deleteFiles(key.toString());

    }
}
