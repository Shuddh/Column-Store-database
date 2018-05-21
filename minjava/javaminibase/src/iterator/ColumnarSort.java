package iterator;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.io.*;
import columnar.ColumnDataPageInfo;
import columnar.Columnarfile;
import columnar.TupleScan;
import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
import tests.*;
import bitmap.*;
import java.util.Random;



public class ColumnarSort{

    public void main(String[] args) throws Exception {
        String COLUMNDBNAME = args[0];
        String COLUMNARFILENAME = args[1];
        String SortField=args[2];
        String Order = args[3];
        int Num_buf = Integer.parseInt(args[4]);

        ColumnDataPageInfo meta = new ColumnDataPageInfo();
        ColumnDataPageInfo cfmeta = meta.ColumnDataPageInfo(COLUMNARFILENAME + ".hdr");

        AttrType[] AllColAttr = cfmeta.getAttrTypes();
        String[] AllCols = cfmeta.getColNames();
        int[] AllColindexes = cfmeta.getColindexes(AllCols);


        String[] temp = new String[1];
        temp[0]= SortField;
        int SortIndex = cfmeta.getColindexes(temp)[0];

        int[] temp1 = new int[1];
        temp1[0]= SortIndex;

        AttrType[] SortAtrr = cfmeta.getcolAttrTypes(temp1);
        short[] SortSize=new short[1];
        if(SortAtrr[0].attrType == AttrType.attrString)
            SortSize[0]=(short) globalVar.sizeOfStr;

        String SortFile=cfmeta.getHffile(SortIndex);

        FldSpec[] projlist = new FldSpec[1];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);

        TupleOrder[] order = new TupleOrder[1];
        if(Order.equalsIgnoreCase("des"))
            order[0] = new TupleOrder(TupleOrder.Descending);
        else
            order[0] = new TupleOrder(TupleOrder.Ascending);

        int SortFieldLength=0;
        if(SortAtrr[0].attrType == AttrType.attrString)
            SortFieldLength=globalVar.sizeOfStr;
        else
            SortFieldLength=globalVar.sizeOfInt;


        AttrType[] New_in=new AttrType[2];
        New_in[0]=SortAtrr[0];
        New_in[1]= new AttrType(AttrType.attrInteger);

        Colscan fscan = null;

        try {
        //    fscan = new FileScan(SortFile, SortAtrr, SortSize, (short) 1, 1, projlist, null);
            fscan = new Colscan(SortFile, SortAtrr, SortSize,(short) 1);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        ColSort sort = null;
        try {
        //    sort = new ColSort(SortAtrr, (short) 1, SortSize, fscan, 1, order[0],SortFieldLength,Num_buf);
            sort = new ColSort(New_in, (short) 2, SortSize, fscan, 1, order[0],SortFieldLength,Num_buf);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

      /*  Tuple t = new Tuple();
        try {
            t.setHdr((short) 1, SortAtrr, SortSize);
        }
        catch (Exception e) {
            e.printStackTrace();
        }*/
       // int size = t.size();

       Tuple t = new Tuple(SortFieldLength+4);
        t.new_setHdr((short) 2, New_in);
       /* try {
            t.setHdr((short) 2, SortAtrr, SortSize);
        }
        catch (Exception e) {
            e.printStackTrace();
        }*/


        t=null;
        String outval = null;

        try {
            t = sort.get_next();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        while (t != null) {

            try {
                if(SortAtrr[0].attrType == AttrType.attrString){
                    int position=Convert.getIntValue(50, t.getTupleByteArray());
                    printCorrespondTuples(AllColindexes,AllColAttr,cfmeta,position);
                }
                else{

                    int position=Convert.getIntValue(4, t.getTupleByteArray());
                    printCorrespondTuples(AllColindexes,AllColAttr,cfmeta,position);
                }

            }
            catch (Exception e) {
                e.printStackTrace();
            }
            try {
                t = sort.get_next();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        // clean up
        try {
            sort.close();
        }
        catch (Exception e) {
     //       e.printStackTrace();
        }

    }
    public static void printCorrespondTuples(int[] columnindexes,AttrType[] AllColAttr,ColumnDataPageInfo cfmeta,int position) throws InvalidSlotNumberException, Exception
    {
        //          System.out.println("position");
        //          System.out.println(position);
        int offset =0;
        RID rid = new RID();
        byte[] data ;
        System.out.print("[");
        for(int i=0,j=0;i<AllColAttr.length;i++)
        {

            if(AllColAttr[i].attrType == AttrType.attrInteger)
            {
                    Heapfile hf = new Heapfile(cfmeta.getHffile(columnindexes[i]));
                    rid = hf.getrid(position);
                    data = hf.getRecord(rid).getTupleByteArray();

                    System.out.print(Convert.getIntValue(offset,data)+",");
            }
            else  if(AllColAttr[i].attrType == AttrType.attrString)
            {
                    Heapfile hf = new Heapfile(cfmeta.getHffile(columnindexes[i]));
                    rid = hf.getrid(position);
                    data = hf.getRecord(rid).getTupleByteArray();

                    System.out.print(Convert.getStrValue(offset,data,globalVar.sizeOfStr).trim()+",");


            }
        }
        System.out.print("]\n");
    }
}