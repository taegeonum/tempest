package vldb.evaluation.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public final class StreamGobbler extends Thread {
    InputStream is;

    // reads everything from is until empty. 
    public StreamGobbler(InputStream is) {
        this.is = is;
    }

    public void run() {
        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line=null;
            while ( (line = br.readLine()) != null)
                System.out.println(line);    
        } catch (IOException ioe) {
            ioe.printStackTrace();  
        }
    }
}