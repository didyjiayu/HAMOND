package blastmapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;

public class OutputHandler extends Thread {

    InputStream inpStr;
    String strType;
    String outputFile;

    public OutputHandler(InputStream inpStr, String strType, String outputFile) {
        this.inpStr = inpStr;
        this.strType = strType;
        this.outputFile = outputFile;
    }

    @Override
    public void run() {
        try {
            InputStreamReader inpStrd = new InputStreamReader(inpStr);
            BufferedReader buffRd = new BufferedReader(inpStrd);
            String line = null;
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputFile)));
            while ((line = buffRd.readLine()) != null) {
                writer.write(line + "\n");
            }
            buffRd.close();
            writer.flush();
            writer.close();

        } catch (Exception e) {
            System.out.println(e);
        }

    }
}
