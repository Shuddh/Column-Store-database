package tests;

import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import btree.AddFileEntryException;
import btree.BT;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.IteratorException;
import btree.KeyNotMatchException;
import btree.PinPageException;
import btree.ScanIteratorException;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.*;
import java.io.*;
import java.util.Arrays;
import java.util.StringTokenizer;

import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.pcounter;


public class batchinsert {

    public static void main (String argv[]) throws Exception {
        int flush=0;
        int diskreads = pcounter.rcounter;
        int diskwrites = pcounter.wcounter;

        AttrType[] attr_types = new AttrType[Integer.parseInt(argv[3])];

        String filepath = "/Users/sravya/Desktop/";

        SystemDefs sysdef = new SystemDefs(argv[1], 10000, 500, "Clock");

        try {
            FileInputStream fileInputStream = new FileInputStream(filepath + argv[0]);
            DataInputStream dataInputStream = new DataInputStream(fileInputStream);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream, "UTF8"));

            String line = bufferedReader.readLine();
            System.out.println(line);

            String[] column_names = new String[Integer.parseInt(argv[3])];
            int string_size[] = new int[Integer.parseInt(argv[3])];
            flush=Integer.parseInt(argv[4]);
            int tuple_length = 0;

            String[] stringArray = line.split("\\s+");

            for (int i = 0; i < stringArray.length; i++) {

                String[] temp = stringArray[i].split(":");
                String[] temp1 = temp[1].split("\\(|\\)");
                column_names[i] = temp[0];
                if (temp1[0].equals("int")) {
                   attr_types[i] = new AttrType(AttrType.attrInteger);
                    tuple_length = tuple_length + globalVar.sizeOfInt;
                    string_size[i] = 4;
                } else {
                    attr_types[i] = new AttrType(AttrType.attrString);
                    string_size[i] = Integer.parseInt(temp1[1]);
                    tuple_length = tuple_length + globalVar.sizeOfStr;
                }

            }
       /*     System.out.println(string_size[0]);
            System.out.println(string_size[1]);
            System.out.println(string_size[2]);
            System.out.println(string_size[3]); */


            Columnarfile cf = new Columnarfile(argv[2], Integer.parseInt(argv[3]), attr_types, column_names, string_size);


            byte[] Tuple_Data = new byte[tuple_length];
            int offset = 0;
            while ((line = bufferedReader.readLine()) != null) {
              //  System.out.println(line);

                String[] temp = line.split("\\s+");
                //      System.out.println(temp[0]);
                int i = 0;
                for (AttrType attr : attr_types) {
                    String value = temp[i];

                    if (attr.attrType == AttrType.attrInteger) {
                        //    	System.out.println(Integer.parseInt(temp[i]));
                        Convert.setIntValue(Integer.parseInt(temp[i]), offset, Tuple_Data);
                        offset = offset + globalVar.sizeOfInt;
                    } else if (attr.attrType == AttrType.attrString) {
                        Convert.setStrValue(temp[i], offset, Tuple_Data);
                        offset = offset + globalVar.sizeOfStr;
                    }
                    i++;
                }
                //      System.out.println(new String(Tuple_Data));

                cf.insertTuple(Tuple_Data);
                offset = 0;

                Arrays.fill(Tuple_Data, (byte) 0);
            }
            //      System.out.println("Disk reads: "+(pcounter.rcounter)+" Disk writes: "+(pcounter.wcounter));

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("records successfully inserted");
        SystemDefs.JavabaseBM.unpinbuffpages();
        if (flush==1) {
        SystemDefs.JavabaseBM.flushAllPages(); }
        System.out.println("Disk reads: " + (pcounter.rcounter - diskreads) + " Disk writes: " + (pcounter.wcounter - diskwrites));
      //  SystemDefs.JavabaseDB.closeDB();
        run cho = new run();
        while (true) {
            String[] CHOICE = cho.main();



         //   System.out.println(CHOICE[0]);
            String[] choice = (CHOICE[1].split(" "));
            flush = Integer.parseInt(choice[(choice.length)-1]);
            try {
                if (CHOICE[0].equals("1")) {
                    pcounter.initialize();
                    index ind = new tests.index();
                    ind.main(CHOICE[1].split(" "));
                }
                if (CHOICE[0].equals("2")) {
                    pcounter.initialize();
                    query qu = new tests.query();
             //       System.out.println(CHOICE[1]);
                    qu.main(CHOICE[1].split(" "));
                }
                if (CHOICE[0].equals("3")) {
                    pcounter.initialize();
                    deletequery del = new tests.deletequery();
                    del.main(CHOICE[1].split(" "));
                }

            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InvalidPageNumberException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (FileIOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (DiskMgrException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
      //      System.out.println("unpin");
            try {
                SystemDefs.JavabaseBM.unpinbuffpages();
                if (flush==1){
                SystemDefs.JavabaseBM.flushAllPages(); }
                SystemDefs.JavabaseDB.closeDB();
                System.out.println("Disk reads: " + (pcounter.rcounter) + " Disk writes: " + (pcounter.wcounter));
            }
            catch (Exception e) {
                e.printStackTrace();
            }
      //      System.out.println("unpined");
        }
    }
    
}
