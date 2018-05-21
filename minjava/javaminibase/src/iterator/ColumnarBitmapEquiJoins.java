package iterator;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
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

public class ColumnarBitmapEquiJoins{
    public int status(String filename) throws Exception{
        if (SystemDefs.JavabaseDB.get_file_entry(filename) == null)
            return -1;
        return 1;
    }

    public String get_accesstypes(String fileName, int columnindexs, int flag) throws Exception {
        int i = 0;
        while (i < 2) {
            if (flag == 1) {
                if (status("BT!" + fileName + '!' + columnindexs) == 1) {
                    return "btree";
                } else {
                    flag = 0;
                    i = i + 1;
                }
            }
            if (flag == 0) {
                if (status(fileName + "Bitmap" + columnindexs) == 1) {
                    return "bitmap";
                } else {
                    flag = 1;
                    i = i + 1;
                }
            }

        }
        return "Columnscan";
    }
    public String[] colNames(String Filter) {
        String[] query = Filter.split("\\||&");
        String[] ColName = new String[query.length];
        for (int j = 0; j < query.length; j++) {
            char[] VALUECONSTRAINT = query[j].toCharArray();
            {
                ColName[j] = "" + VALUECONSTRAINT[0];

            }
        }
        return ColName;

    }
    public int[] access(String Filter){

        String[] temp_filter=Filter.split("\\||&");
        int[] flags=new int[temp_filter.length];
        for(int i=0;i<temp_filter.length;i++){
            if(temp_filter[i].indexOf('<')!=-1 || temp_filter[i].indexOf('>')!=-1){
                flags[i]=1;
            }
            else{
                flags[i]=0;
            }

        }
        return flags;


    }


    public void main(String[] args) throws Exception {
        String Dbname = args[0];
        String OuterfileName = args[1];
        String InnerfileName = args[2];
        String outFilter = args[3];
        String rightFilter = args[4];
        String JoinFilter = args[5];
        String[] outerTargetCols = args[6].split(",");
        String[] innerTargetCols = args[7].split(",");

        String columnarFileName = args[8];
        int amt_of_mem = Integer.parseInt(args[9]);


        ColumnDataPageInfo meta = new ColumnDataPageInfo();
        ColumnDataPageInfo outercfmeta = meta.ColumnDataPageInfo(OuterfileName + ".hdr");
        ColumnDataPageInfo innercfmeta = meta.ColumnDataPageInfo(InnerfileName + ".hdr");



        int[] outercolindex = outercfmeta.getColindexes(outerTargetCols);

        int[] innercolindex = innercfmeta.getColindexes(innerTargetCols);

        AttrType[] outerattrTarget = outercfmeta.getcolAttrTypes(outercolindex);
        AttrType[] innerattrTarget = innercfmeta.getcolAttrTypes(innercolindex);

        String[] outerColName = colNames(outFilter);

        int[] outercolumnindexs = outercfmeta.getColindexes(outerColName);
        String OutAc_types = "";
        int[] out_flags=access(outFilter);

        for (int i = 0; i < outercolumnindexs.length; i++) {
            OutAc_types = OutAc_types + get_accesstypes(OuterfileName, outercolumnindexs[i], out_flags[i]);

            if (i != ((outercolumnindexs.length) - 1))
                OutAc_types = OutAc_types + ",";
        }

        String[] innerColName = colNames(rightFilter);
        int[] innercolumnindexs = innercfmeta.getColindexes(innerColName);
        String InnerAc_types = "";
        int[] in_flags=access(rightFilter);

        for (int i = 0; i < innercolumnindexs.length; i++) {

            InnerAc_types = InnerAc_types + get_accesstypes(InnerfileName, innercolumnindexs[i], in_flags[i]);
            if (i != ((innercolumnindexs.length) - 1))
                InnerAc_types = InnerAc_types + ",";
        }
        String Out_Arg = Dbname + " " + OuterfileName + " " + outerColName[0] + " " + outFilter + " " + "500" + " " + OutAc_types + " " + "1" + " " + "1";
        String Inner_Arg = Dbname + " " + InnerfileName + " " + innerColName[0] + " " + rightFilter + " " + "500" + " " + InnerAc_types + " " + "-1" + " " + "1";

        ColumnarIndexScan Innercolscan = new tests.ColumnarIndexScan();
        Innercolscan.main(Inner_Arg.split(" "));

        ColumnarIndexScan Outercolscan = new tests.ColumnarIndexScan();
        Outercolscan.main(Out_Arg.split(" "));

        String[] Joinquery = JoinFilter.split("=");
        String[] temp1 = new String[1];
        temp1[0]= Joinquery[0];
        String[] temp2 = new String[1];
        temp2[0]= Joinquery[1];
        int join_out_index = outercfmeta.getColindexes(temp1)[0];
        int join_in_index = innercfmeta.getColindexes(temp2)[0];

        Heapfile Outer_Bitmap_metafile = new Heapfile(OuterfileName + "Bitmap" + join_out_index);
        Scan Outer_hfscan = Outer_Bitmap_metafile.openScan();
        //   Heapfile Inner_Bitmap_metafile = new Heapfile(InnerfileName+"Bitmap"+join_in_index);
        //  Scan Inner_hfscan =Inner_Bitmap_metafile.openScan();

        RID out_rid = new RID();
        // RID in_rid = new RID();

        Tuple t = null;
        ArrayList<ArrayList<Integer>> BEJ_Outer_Positions = new ArrayList<ArrayList<Integer>>();
        ArrayList<ArrayList<Integer>> BEJ_Inner_Positions = new ArrayList<ArrayList<Integer>>();

        while ((t = Outer_hfscan.getNext(out_rid)) != null) {

            String Value = Convert.getStrValue(0, t.getTupleByteArray(), globalVar.sizeOfStr);
            String outer_bmfName = "BM!" + OuterfileName + '!' + join_out_index + '!' + Value;
            String inner_bmfName = "BM!" + InnerfileName + '!' + join_in_index + '!' + Value;
                if(status(inner_bmfName)!=-1){
                    BitMapFile outer_bmf = new BitMapFile(outer_bmfName);
                    BitMapFile inner_bmf = new BitMapFile(inner_bmfName);
                    BEJ_Outer_Positions.add(outer_bmf.getPos(outer_bmfName));
                    BEJ_Inner_Positions.add(inner_bmf.getPos(inner_bmfName));
                }

            }
        ArrayList<Integer> Out_Value = new ArrayList<Integer>();
        ArrayList<Integer> In_Value = new ArrayList<Integer>();

        Heapfile Out_file = new Heapfile("NJP_outerJoin");
        RID rid1=new RID();
        Scan hScan1=Out_file.openScan();
        Tuple hTuple1=null;
        while((hTuple1=hScan1.getNext(rid1))!=null) {
            Out_Value.add(Convert.getIntValue(0, hTuple1.getTupleByteArray()));
        }
        Heapfile In_file = new Heapfile("NJP_InnerJoin");
        RID rid2=new RID();
        Scan hScan2=In_file.openScan();
        Tuple hTuple2=null;
        while((hTuple2=hScan2.getNext(rid2))!=null) {
            In_Value.add(Convert.getIntValue(0, hTuple2.getTupleByteArray()));
        }
        for(int i=0;i<BEJ_Outer_Positions.size();i++){
            (BEJ_Outer_Positions.get(i)).retainAll(Out_Value);
        }
        for(int i=0;i<BEJ_Inner_Positions.size();i++){
            (BEJ_Inner_Positions.get(i)).retainAll(In_Value);
        }
        printCorrespondTuples(outercolindex,innercolindex, outerattrTarget, innerattrTarget, outercfmeta,innercfmeta,BEJ_Outer_Positions,BEJ_Inner_Positions);
    }
    public static void printCorrespondTuples(int[] outercolindexes,int[] incolindexes, AttrType[] outerattrTarget, AttrType[] innerattrTarget,ColumnDataPageInfo outcfmeta,ColumnDataPageInfo incfmeta,ArrayList<ArrayList<Integer>> Outer_Positions ,ArrayList<ArrayList<Integer>> Inner_Positions) throws InvalidSlotNumberException, Exception {

        for(int op=0; op<Outer_Positions.size();op++) {

            for (int Outposition : Outer_Positions.get(op)) {

                String ot = "";
                for (int i = 0; i < outerattrTarget.length; i++) {
                    byte[] data;

                    Heapfile outhf = new Heapfile(outcfmeta.getHffile(outercolindexes[i]));

                    RID rid = outhf.getrid(Outposition);
                    data = outhf.getRecord(rid).getTupleByteArray();
                    if (outerattrTarget[i].attrType == AttrType.attrInteger) {

                        ot = ot + Integer.toString(Convert.getIntValue(0, data)) + ",";


                    } else if (outerattrTarget[i].attrType == AttrType.attrString) {
                        ot = ot + Convert.getStrValue(0, data, globalVar.sizeOfStr).trim() + ",";

                    }

                }

                for (int inposition : Inner_Positions.get(op)) {

                    System.out.print("[");

                    System.out.print(ot);
                    byte[] data;

                    for (int i = 0; i < innerattrTarget.length; i++) {

                        Heapfile hf = new Heapfile(incfmeta.getHffile(incolindexes[i]));

                        RID rid = hf.getrid(inposition);
                        data = hf.getRecord(rid).getTupleByteArray();
                        if (innerattrTarget[i].attrType == AttrType.attrInteger) {

                            System.out.print(Convert.getIntValue(0, data) + ",");

                        } else if (innerattrTarget[i].attrType == AttrType.attrString) {

                            System.out.print(Convert.getStrValue(0, data, globalVar.sizeOfStr).trim() + ",");

                        }

                    }
                    System.out.print("]\n");

                }
            }
        }

    }


}