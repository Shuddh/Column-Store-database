package tests;

import java.io.File;
import java.io.IOException;

import bitmap.*;
import btree.*;
import bufmgr.*;
import columnar.*;
import diskmgr.*;
import global.*;
import heap.*;


public class index implements GlobalConst{

    private static BitMapFile bmf = null;
    private static BTreeFile btf=null;
    private static String columnDbName;
    private static String columnarFileName;
    private static String[] columnName = {""};
    private static String indexType;
    private static KeyClass value;
//    private static int startRead = 0, startWrite = 0;
    private static Columnarfile f = null;
    private static String []colNames;

    /**
     * @param args
     * @throws IOException 
     * @throws HFDiskMgrException 
     * @throws HFBufMgrException 
     * @throws HFException 
     */
    public void index() throws HFException, HFBufMgrException, HFDiskMgrException, IOException{
    	
    }
    public void main(String[] args) throws InvalidSlotNumberException, Exception {
        // TODO Auto-generated method stub
        int  startRead = pcounter.rcounter;
        int  startWrite = pcounter.wcounter;
        System.out.println(startRead+" "+startWrite);
        AttrType[] types = new AttrType[4];

        types[0] = new AttrType(AttrType.attrInteger);
        types[1] = new AttrType(AttrType.attrString);
        types[2] = new AttrType(AttrType.attrString);
        types[3] = new AttrType(AttrType.attrInteger);

        
        columnDbName=args[0];
        columnarFileName=args[1];
        columnName[0]=args[2];
        indexType=args[3];

      //  System.out.println(args[0]);
     //   System.out.println(args[1]);
     //   System.out.println(args[2]);
     //   System.out.println(args[3]);
      //  columnValue=args[5];

       SystemDefs.JavabaseDB.openDB(columnDbName);
        ColumnDataPageInfo meta = new ColumnDataPageInfo();
        ColumnDataPageInfo cfmeta = meta.ColumnDataPageInfo(columnarFileName+".hdr");
        String[] columnNames = cfmeta.getColNames();

        System.out.println ("\n  Begin Index Test: \n");
        final boolean OK = true;
        final boolean FAIL = false;
        int choice=100;
        final int reclen = 128;

        boolean status = OK;
        TID tid = new TID();

        int ColumnIndex= (cfmeta.getColindexes(columnName))[0];
        System.out.println(cfmeta.getHffile(ColumnIndex));
        Heapfile reqHFile=new Heapfile(cfmeta.getHffile(ColumnIndex));
        if(indexType.equalsIgnoreCase("Btree"))
        {
            try
            {
                RID rid=new RID();
                Scan hScan=reqHFile.openScan();
                Tuple hTuple=null;
                System.out.println("btree name: "+"BT"+columnarFileName+ColumnIndex);
                try{
                    SystemDefs.JavabaseDB.delete_file_entry("BT"+columnarFileName+ColumnIndex);
                }catch(Exception e)
                {
                }

                btf = new BTreeFile("BT!"+columnarFileName+'!'+ColumnIndex, AttrType.attrInteger, globalVar.sizeOfInt, 0/*delete*/);

                while((hTuple=hScan.getNext(rid))!=null)
                {
                    int temp=Convert.getIntValue(0, hTuple.getTupleByteArray());
                   
                    btf.insert(new btree.IntegerKey(temp), rid);
                }
                BT.printBTree(btf.getHeaderPage());
                System.out.println("printing leaf pages...");
                BT.printAllLeafPages(btf.getHeaderPage());
            //    SystemDefs.JavabaseBM.unpinPage(, true);
                System.out.println("created Btree index...");
            } catch (Exception e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
        else if(indexType.equalsIgnoreCase("Bitmap"))
        {
            try
            {
                RID rid=new RID();
                Scan hScan=reqHFile.openScan();
                Tuple hTuple=null;
                Heapfile UNIQUeValhf =
               while((hTuple=hScan.getNext(rid))!=null) {
                    AttrType attr;
         
                    KeyClass columnValue = null;
              
                    attr= (cfmeta.getAttrTypes())[ColumnIndex];
                    String bitName = null;
                    if(attr.attrType == AttrType.attrInteger)
                    {
                        int colValue = Convert.getIntValue(0, hTuple.getTupleByteArray());
                        columnValue= new IntegerKey(colValue);
                        bitName = "BM!" + columnarFileName + '!' + ColumnIndex + '!'+ Integer.toString(colValue);
                        System.out.println(bitName);

                    }
                    else if(attr.attrType == AttrType.attrString) {
                        String colValue = Convert.getStrValue(0, hTuple.getTupleByteArray(), globalVar.sizeOfStr);
                        columnValue = new StringKey(colValue);
                        bitName = "BM!" + columnarFileName + '!' + ColumnIndex + '!' + colValue;
                        System.out.println(bitName);

                    }
                        PageId  pageid = null;
                         pageid =SystemDefs.JavabaseDB.get_file_entry(bitName);

                        if(pageid==null) {


                            bmf = new BitMapFile(bitName, reqHFile, columnValue);
                            bmf.printBitMapFile(bitName);
                            System.out.println("created Bitmap index...");
                        }
                }
            } catch (Exception e)
            {
                // TODO Auto-generated catch block
               // e.printStackTrace();
            }

        }
        System.out.println("end of test....");
     //   System.out.println(pcounter.rcounter+" and "+pcounter.wcounter);

  /*  deletequery del = new tests.deletequery();
   query ind = new tests.query();
    try {
    //	del.main("colDB2 Colfile!1 A,C C==6 150 filescan 1".split(" "));
		ind.main("colDB17 Colfile!17 A,B,C,D C==6 500 filescan".split(" "));
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
	
	} */
    }
}