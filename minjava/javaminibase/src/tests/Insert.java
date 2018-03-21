import global.AttrType;

import java.io.*;

public class Insert {
    public static void main (String argv[])
    {
        String filepath = "C:/Users/varun ch/Desktop/HASITHA/academix/ASU/DBMSI/minjava/javaminibase/src/tests/";

        String[] colnames = new String[Integer.parseInt(argv[3])];
        AttrType[] type = new AttrType[Integer.parseInt(argv[3])];
        int startread, startwrite; /* keeps track of reads and writes before batchinsert starts */

        try {
            FileInputStream fin = new FileInputStream(filepath+argv[0]);
            DataInputStream din = new DataInputStream(fin);
            BufferedReader bin = new BufferedReader(new InputStreamReader(din));

            /* code that reads first line, reads schema and creates Columnarfile */
            String line = bin.readLine();

            //StringTokenizer st = new StringTokenizer(line);
            String[] stringArray = line.split(" ");
            int i = 0, tuplelength = 0;
            System.out.print(stringArray[1]);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

