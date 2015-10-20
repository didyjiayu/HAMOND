package blastmapreduce;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RunnerMap extends Mapper<String, String, IntWritable, Text> {

    private String localDB = "";
    private String localBlastProgram = "";

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        URI[] local = context.getCacheArchives();
        this.localDB = local[0].getPath();
        this.localBlastProgram = local[0].getPath();
    }

    @Override
    public void map(String key, String value, Context context) throws IOException,
            InterruptedException {

        Configuration conf = context.getConfiguration();
        String programDir = conf.get(DataAnalysis.QUERY);
        String execName = conf.get(DataAnalysis.DATABASE);
        String cmdArgs = conf.get(DataAnalysis.OUTPUT);

        // We have the full file names in the value.
        String[] tmp = value.split(File.separator);
        String fileNameOnly = tmp[tmp.length - 1];// Last part should be the
        // file name.

        String localInputFile = workingDir + File.separator + fileNameOnly;
        String outFile = workingDir + File.separator + fileNameOnly + ".out";
        String stdOutFile = workingDir + File.separator + fileNameOnly + ".stdout";
        String stdErrFile = workingDir + File.separator + fileNameOnly + ".stderr";

        // download the file from HDFS
        Path inputFilePath = new Path(value);
        FileSystem fs = inputFilePath.getFileSystem(conf);
        fs.copyToLocalFile(inputFilePath, new Path(localInputFile));

        // Prepare the arguments to the executable
        String execCommand = cmdArgs.replaceAll("#_INPUTFILE_#", localInputFile);
        if (cmdArgs.contains("#_OUTPUTFILE_#")) {
            execCommand = execCommand.replaceAll("#_OUTPUTFILE_#", outFile);
        } else {
            outFile = stdOutFile;
        }

        execCommand = this.localBlastProgram + File.separator + execName + " " + execCommand + " -db " + this.localDB;
		//Create the external process

        Process p = Runtime.getRuntime().exec(execCommand);

        OutputHandler inputStream = new OutputHandler(p.getInputStream(), "INPUT", stdOutFile);
        OutputHandler errorStream = new OutputHandler(p.getErrorStream(), "ERROR", stdErrFile);

        // start the stream threads.
        inputStream.start();
        errorStream.start();

        p.waitFor();

        //Upload the results to HDFS

        Path outputDirPath = new Path(outputDir);
        Path outputFileName = new Path(outputDirPath, fileNameOnly);
        fs.copyFromLocalFile(new Path(outFile), outputFileName);

    }
}
