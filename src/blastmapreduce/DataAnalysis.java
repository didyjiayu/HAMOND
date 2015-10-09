/**
 * Software License, Version 1.0
 *
 * Copyright 2003 The Trustees of Indiana University. All rights reserved.
 *
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1) All redistributions of source code must retain the above copyright notice,
 * the list of authors in the original source code, this list of conditions and
 * the disclaimer listed in this license; 2) All redistributions in binary form
 * must reproduce the above copyright notice, this list of conditions and the
 * disclaimer listed in this license in the documentation and/or other materials
 * provided with the distribution; 3) Any documentation included with all
 * redistributions must include the following acknowledgment:
 *
 * "This product includes software developed by the Community Grids Lab. For
 * further information contact the Community Grids Lab at
 * http://communitygrids.iu.edu/."
 *
 * Alternatively, this acknowledgment may appear in the software itself, and
 * wherever such third-party acknowledgments normally appear.
 *
 * 4) The name Indiana University or Community Grids Lab or NaradaBrokering,
 * shall not be used to endorse or promote products derived from this software
 * without prior written permission from Indiana University. For written
 * permission, please contact the Advanced Research and Technology Institute
 * ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202. 5) Products
 * derived from this software may not be called NaradaBrokering, nor may Indiana
 * University or Community Grids Lab or NaradaBrokering appear in their name,
 * without prior written permission of ARTI.
 *
 *
 * Indiana University provides no reassurances that the source code provided
 * does not infringe the patent or any other intellectual property rights of any
 * other entity. Indiana University disclaims any liability to any recipient for
 * claims brought by any other entity based on infringement of intellectual
 * property rights or otherwise.
 *
 * LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO
 * WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 * NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF
 * INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS.
 * INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS",
 * "VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.
 * LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR
 * ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION
 * GENERATED USING SOFTWARE.
 */
package blastmapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.net.URI;

/**
 * Cap3 Data analysis using Hadoop MapReduce. This program demonstrated a usage
 * of "map-only" operation to execute a data analysis application on a
 * collection of data files.
 *
 * Cap3 is a gene sequencing program which consumes a .fsa file and produces
 * several output files along with the standard out.
 *
 * The data is placed in a shared file system (or can be placed on all the local
 * disks) and the file names are written to HDFS. For hadoop, the data file
 * names becomes the data.
 *
 * Hadoop executes each map task by passing a data file name as the value
 * parameter. Map task execute Cap3 program (written in C) and save the standard
 * output into a file. It can also be used to copy these output files to a
 * predefined location.
 *
 * @author Jaliya Ekanayake (jekanaya@cs.indiana.edu) 03/03/2009
 *
 * @author Thilina Gunarathne (tgunarat@cs.indiana.edu) 2009-2010
 *
 *
 * @editor Stephen, TAK-LON WU (taklwu@indiana.edu) 2010
 */
public class DataAnalysis extends Configured implements Tool {

    public static String WORKING_DIR = "working_dir";
    public static String OUTPUT_DIR = "out_dir";
    public static String EXECUTABLE = "exec_name";
    public static String PROGRAM_DIR = "exec_dir";
    public static String PARAMETERS = "params";
    public static String DB_NAME = "nr";
    public static String DB_ARCHIVE = "BlastDB.tar.gz";

    /**
     * Launch the MapReduce computation. This method first, remove any previous
     * working directories and create a new one Then the data (file names) is
     * copied to this new directory and launch the MapReduce (map-only though)
     * computation.
     *
     * @param numMapTasks - Number of map tasks.
     * @param numReduceTasks - Number of reduce tasks =0.
     * @param programDir - The directory where the Cap3 program is.
     * @param execName - Name of the executable.
     * @param dataDir - Directory where the data is located.
     * @param outputDir - Output directory to place the output.
     * @param cmdArgs - These are the command line arguments to the Cap3
     * program.
     * @throws Exception - Throws any exception occurs in this program.
     */
    void launch(int numReduceTasks, String programDir,
            String execName, String workingDir, String databaseArchive, String databaseName, String dataDir, String outputDir, String cmdArgs) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "execName");

        // First get the file system handler, delete any previous files, add the
        // files and write the data to it, then pass its name as a parameter to
        // job
        Path hdMainDir = new Path(outputDir);
        FileSystem fs = FileSystem.get(conf);
        fs.delete(hdMainDir, true);

        Path hdOutDir = new Path(hdMainDir, "out");

        // Starting the data analysis.
        Configuration jc = job.getConfiguration();

        jc.set(WORKING_DIR, workingDir);
        jc.set(EXECUTABLE, execName);
        jc.set(PROGRAM_DIR, programDir); // this the name of the executable archive
        jc.set(DB_ARCHIVE, databaseArchive);
        jc.set(DB_NAME, databaseName);
        jc.set(PARAMETERS, cmdArgs);
        jc.set(OUTPUT_DIR, outputDir);

        // using distributed cache
        // flush it
        //DistributedCache.releaseCache(new URI(programDir), jc);
        //DistributedCache.releaseCache(new URI(databaseArchive), jc);
        //DistributedCache.purgeCache(jc);
        // reput the data into cache
        long startTime = System.currentTimeMillis();
        //DistributedCache.addCacheArchive(new URI(databaseArchive), jc);
        job.addCacheArchive(new URI(programDir));
        System.out.println("Add Distributed Cache in "
                + (System.currentTimeMillis() - startTime) / 1000.0
                + " seconds");

        FileInputFormat.setInputPaths(job, dataDir);
        FileOutputFormat.setOutputPath(job, hdOutDir);

        job.setJarByClass(DataAnalysis.class);
        job.setMapperClass(RunnerMap.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(DataFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(numReduceTasks);

        startTime = System.currentTimeMillis();

        int exitStatus = job.waitForCompletion(true) ? 0 : 1;
        System.out.println("Job Finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0
                + " seconds");
        //clean the cache

        System.exit(exitStatus);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 8) {
            System.err.println("Usage: DataAnalysis <Executable and Database Archive on HDFS> <Executable> <Working_Dir> <Database dir under archive> <Database name> <HDFS_Input_dir> <HDFS_Output_dir> <Cmd_args>");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        String programDir = args[0];
        String execName = args[1];
        String workingDir = args[2];
        String databaseArchive = args[3];
        String databaseName = args[4];
        String inputDir = args[5];
        String outputDir = args[6];
        //"#_INPUTFILE_# -p 95 -o 49 -t 100"
        String cmdArgs = args[7];

        int numReduceTasks = 0;// We don't need reduce here.

        launch(numReduceTasks, programDir, execName, workingDir, databaseArchive, databaseName, inputDir,
                outputDir, cmdArgs);
        return 0;
    }

    public static void main(String[] argv) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DataAnalysis(), argv);
        System.exit(res);
    }
}
