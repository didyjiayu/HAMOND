/*
 * Copyright 2016 YU Jia

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package diamondmapreduce;

/**
 *
 * @author yujia1986
 */
import awshamondsidefunctions.AWSDiamondReducer;
import java.net.URI;
import java.util.Arrays;
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
import sharedsidefunctions.CheckArguments;
import sharedsidefunctions.CopyFromLocal;
import sharedsidefunctions.DeleteHDFSFiles;
import sharedsidefunctions.HadoopUser;
import sharedsidefunctions.MakeDB;
import sharedsidefunctions.MakeHamondHDFSdir;

public class DiamondMapReduce extends Configured implements Tool {

    public static String QUERY = "query";
    public static String DATABASE = "reference";
    public static String OUTPUT = "output";
    public static String userName;

    int launchHamond(String[] arguments) throws Exception {

        //extract diamond, query, reference and output from array
        String diamond = arguments[0];
        String query = arguments[1];
        String dataBase = arguments[2];
        String outPut = arguments[3];

        //set Hadoop configuration
        Job job = Job.getInstance(getConf(), "DIAMOND");
        Configuration conf = job.getConfiguration();
        hamondsidefunctions.SetConf.setHadoopConf(conf);

        //get user name
        userName = HadoopUser.getHadoopUser();

        //delete all existing DIAMOND files under current Hadoop user
        DeleteHDFSFiles.deleteAllFiles(userName);

        //make Hamond directory on HDFS
        MakeHamondHDFSdir.makedir(conf, userName);

        //make DIAMOND database on local then copy to HDFS with query and delete local database
        MakeDB.makeDB(diamond, dataBase);

        //copy DIAMOND bin, query and local database file to HDFS
        CopyFromLocal.copyFromLocal(conf, diamond, query, dataBase, userName);

        //pass query name and database name to mappers
        conf.set(QUERY, query);
        conf.set(DATABASE, dataBase + ".dmnd");
        String[] subArgs = Arrays.copyOfRange(arguments, 4, arguments.length);
        conf.setStrings("DIAMOND-arguments", subArgs);
        conf.setStrings(OUTPUT, outPut);

        //add DIAMOND bin and database into distributed cache
        job.addCacheFile(new URI("/user/" + userName + "/Hamond/diamond"));
        job.addCacheFile(new URI("/user/" + userName + "/Hamond/" + new Path(dataBase).getName() + ".dmnd"));

        //set job input and output paths
        FileInputFormat.addInputPath(job, new Path("/user/" + userName + "/Hamond/" + new Path(query).getName()));
        FileOutputFormat.setOutputPath(job, new Path("/user/" + userName + "/Hamond/out"));

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
    
    int launchHamondAWS(String[] arguments) throws Exception {

        //extract diamond, query, reference and output from array
        String diamond = arguments[0];
        String query = arguments[1];
        String dataBase = arguments[2];
        String outPut = arguments[3];

        //set Hadoop configuration
        Job job = Job.getInstance(getConf(), "DIAMOND");
        Configuration conf = job.getConfiguration();
        awshamondsidefunctions.SetConf.setHadoopConf(conf);

        //get user name
        userName = HadoopUser.getHadoopUser();
        
        //delete all existing DIAMOND files under current Hadoop user
        DeleteHDFSFiles.deleteAllFiles(userName);
        
        //make local Hamond dir
        awshamondsidefunctions.MakeHamondDir.make();

        //copy DIAMOND, query, reference from S3 to master local
        awshamondsidefunctions.CopyFromS3.copyFromS3(diamond, query, dataBase);

        //make Hamond directory on HDFS
        MakeHamondHDFSdir.makedir(conf, userName);

        //make DIAMOND database on local then copy to HDFS with query and delete local database
        MakeDB.makeDB("/mnt/Hamond/diamond", "/mnt/Hamond/" + new Path(dataBase).getName());
        
        //copy DIAMOND bin, query and local database file to HDFS
        CopyFromLocal.copyFromLocal(conf, "/mnt/Hamond/diamond", "/mnt/Hamond/" + new Path(query).getName(), "/mnt/Hamond/" + new Path(dataBase).getName(), userName);

        //pass query name and database name to mappers
        conf.set(QUERY, query);
        conf.set(DATABASE, dataBase);
        conf.set(OUTPUT, outPut);
        String[] subArgs = Arrays.copyOfRange(arguments, 4, arguments.length);
        conf.setStrings("DIAMOND-arguments", subArgs);
        conf.setStrings(OUTPUT, outPut);

        //add DIAMOND bin and database into distributed cache
        job.addCacheFile(new URI("/user/" + userName + "/Hamond/diamond"));
        job.addCacheFile(new URI("/user/" + userName + "/Hamond/" + new Path(dataBase).getName() + ".dmnd"));

        //set job input and output paths
        FileInputFormat.addInputPath(job, new Path("/user/" + userName + "/Hamond/" + new Path(query).getName()));
        FileOutputFormat.setOutputPath(job, new Path("/user/" + userName + "/Hamond/out"));

        //set job driver and mapper
        job.setJarByClass(DiamondMapReduce.class);
        job.setMapperClass(DiamondMapper.class);
        job.setReducerClass(AWSDiamondReducer.class);

        //set job input format into customized multilines format
        job.setInputFormatClass(CustomNLineFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);
        job.setSpeculativeExecution(false);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    @Override
    public int run(String[] args) throws Exception {
        CheckArguments.check(args);
        //check whether to invoke regular Hamond or HamondAWS
        int status = 100;
        if (!args[0].contains("s3")) {
            status = launchHamond(args);
            hamondsidefunctions.CopyToLocal.copyToLocal(args[3]);
            DeleteHDFSFiles.deleteAllFiles(userName);
        } else if (args[0].contains("s3")) {
            status = launchHamondAWS(args);
            awshamondsidefunctions.CopyToS3.copyToS3(args[3]);
            DeleteHDFSFiles.deleteAllFiles(userName);
            awshamondsidefunctions.DeleteLocalFiles.delete();
        }
        return status;
    }

    public static void main(String[] argv) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DiamondMapReduce(), argv);
        System.exit(res);
    }
}
