package tests;

import diskmgr.pcounter;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.pcounter;

import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

import java.io.*;
import global.*;


public class rundb{

    public static void main (String argv[]) throws Exception {
        int flush=0;
        run cho = new run();
            while (true) {
                try {
                    String[] CHOICE = cho.main();


                    //   System.out.println(CHOICE[0]);
                    String[] choice = (CHOICE[1].split(" "));
                    flush = Integer.parseInt(choice[(choice.length) - 1]);
                    try {
                        if (CHOICE[0].equals("1")) {
                            pcounter.initialize();
                            batchinsert bi = new tests.batchinsert();
                            bi.main(CHOICE[1].split(" "));
                        }

                        if (CHOICE[0].equals("2")) {
                            pcounter.initialize();
                            index ind = new tests.index();
                            ind.main(CHOICE[1].split(" "));
                        }
                        if (CHOICE[0].equals("3")) {
                            pcounter.initialize();
                            query qu = new tests.query();
                            //       System.out.println(CHOICE[1]);
                            qu.main(CHOICE[1].split(" "));
                        }
                        if (CHOICE[0].equals("4")) {
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
                        if (flush == 1) {
                            SystemDefs.JavabaseBM.flushAllPages();
                        }
                        SystemDefs.JavabaseDB.closeDB();
                        System.out.println("Disk reads: " + (pcounter.rcounter) + " Disk writes: " + (pcounter.wcounter));
                    } catch (Exception e) {
            //            e.printStackTrace();
                    }
                }
                catch (Exception e) {
            //        e.printStackTrace();
                }
                continue;

            }
    }
}