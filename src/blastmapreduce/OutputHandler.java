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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Handles the writing of a stream to a file. This is used to capture the output
 * of stdout and stderr when external programs are executed from map/reduce
 * functions.
 *
 * @author Jaliya Ekanayake (jekanaya@cs.indiana.edu) 03/03/2009
 *
 */
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
