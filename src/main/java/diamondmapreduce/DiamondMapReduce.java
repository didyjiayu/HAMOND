package diamondmapreduce;

/**
 *
 * @author yujia1986
 */
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import sidefunctions.CopyFromLocal;
import sidefunctions.CopyToLocal;
import sidefunctions.DeleteHDFSFiles;
import sidefunctions.HadoopUser;
import sidefunctions.MakeDB;
import sidefunctions.MakeHamondDir;
import sidefunctions.RemoveDB;
import sidefunctions.SetConf;

public class DiamondMapReduce extends Configured implements Tool {

    public static String QUERY = "query_sequence";
    public static String DATABASE = "database";
    public static String userName;

    int launch(String diamond, String query, String dataBase, String outPut) throws Exception {

        //set Hadoop configuration
        Job job = Job.getInstance(getConf(), "DIAMOND");
        Configuration conf = job.getConfiguration();
        SetConf.setHadoopConf(conf);

        //get user name
        userName = HadoopUser.getHadoopUser();
        
        //delete all existing DIAMOND files under current Hadoop user
        DeleteHDFSFiles.deleteAllFiles(userName);
        
        //make Hamond directory on HDFS
        MakeHamondDir.makedir(conf, userName);

        //make DIAMOND database on local then copy to HDFS with query and delete local database
//        MakeDB.makeDB(System.getProperty("user.dir")+"/diamond", dataBase);
        MakeDB.makeDB(diamond, dataBase);
        
        //copy DIAMOND bin, query and local database file to HDFS
        CopyFromLocal.copyFromLocal(conf, diamond, query, dataBase, userName);
        
        //remove local database file
        RemoveDB.removeDB(dataBase + ".dmnd");

        //pass query name and database name to mappers
        conf.set(QUERY, query);
        conf.set(DATABASE, dataBase + ".dmnd");

        //add DIAMOND bin and database into distributed cache
        job.addCacheFile(new URI("/user/"+userName+"/Hamond/diamond"));
        job.addCacheFile(new URI("/user/"+userName+"/Hamond/" + new Path(dataBase).getName() + ".dmnd"));

        //set job input and output paths
        FileInputFormat.addInputPath(job, new Path("/user/"+userName + "/Hamond/" + new Path(query).getName()));
        FileOutputFormat.setOutputPath(job, new Path("/user/"+userName + "/Hamond/output"));

        //set job driver and mapper
        job.setJarByClass(DiamondMapReduce.class);
        job.setMapperClass(DiamondMapper.class);

        //set job input format into customized multilines format
        job.setInputFormatClass(CustomNLineFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(0);
        job.setSpeculativeExecution(false);
        
        return job.waitForCompletion(true) ? 0 : 1;
        
    }

    @Override
    public int run(String[] args) throws Exception {
        String dmd = args[0];
        String q = args[1];
        String db = args[2];
        String op = args[3];
        int status = launch(dmd, q, db, op);
        CopyToLocal.copyToLocal(op);
        DeleteHDFSFiles.deleteAllFiles(userName);
        return status;
    }

    public static void main(String[] argv) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DiamondMapReduce(), argv);
        System.exit(res);
    }
}
