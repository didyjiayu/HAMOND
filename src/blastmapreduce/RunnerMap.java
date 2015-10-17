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

/**
 * @author Thilina Gunarathne (tgunarat@cs.indiana.edu)
 *
 * @editor Stephen, TAK-LON WU (taklwu@indiana.edu)
 */
public class RunnerMap extends Mapper<String, String, IntWritable, Text> {

    private String localDB = "";
    private String localBlastProgram = "";

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        URI[] local = context.getCacheArchives();
        this.localDB = local[0].getPath() + File.separator + conf.get(DataAnalysis.DB_ARCHIVE) + File.separator + conf.get(DataAnalysis.DB_NAME);
        this.localBlastProgram = local[0].getPath();
        //this.localBlastProgram = "/N/u/taklwu/Quarry/blast";
    }

    @Override
    public void map(String key, String value, Context context) throws IOException,
            InterruptedException {

        Configuration conf = context.getConfiguration();
        String programDir = conf.get(DataAnalysis.PROGRAM_DIR);
        String execName = conf.get(DataAnalysis.EXECUTABLE);
        String cmdArgs = conf.get(DataAnalysis.PARAMETERS);
        String outputDir = conf.get(DataAnalysis.OUTPUT_DIR);
        String workingDir = conf.get(DataAnalysis.WORKING_DIR);

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
