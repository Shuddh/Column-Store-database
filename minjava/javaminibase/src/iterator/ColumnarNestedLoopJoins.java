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

public class ColumnarNestedLoopJoins {


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


    public void main(String[] args) throws Exception {
        String Dbname = args[0];
        String OuterfileName = args[1];
        String InnerfileName = args[2];
        String outFilter = args[3];
        String rightFilter = args[4];
        String JoinFilter = args[5];
        String Accesstype = args[6];
        String[] outerTargetCols = args[7].split(",");
        String[] innerTargetCols = args[8].split(",");

        String columnarFileName = args[9];
        int amt_of_mem = Integer.parseInt(args[10]);


        ColumnDataPageInfo meta = new ColumnDataPageInfo();
        ColumnDataPageInfo outercfmeta = meta.ColumnDataPageInfo(OuterfileName + ".hdr");
        ColumnDataPageInfo innercfmeta = meta.ColumnDataPageInfo(InnerfileName + ".hdr");
        int[] outercolindex = outercfmeta.getColindexes(outerTargetCols);
        int[] innercolindex = innercfmeta.getColindexes(innerTargetCols);
        AttrType[] outerattrTarget = outercfmeta.getcolAttrTypes(outercolindex);
        AttrType[] innerattrTarget = innercfmeta.getcolAttrTypes(innercolindex);

        String[] outerColName = colNames(outFilter);

        int[] outercolumnindexs = outercfmeta.getColindexes(outerColName);
        int Access_status = -1;
        if (Accesstype.equalsIgnoreCase("Btree"))
            Access_status = 1;
        if (Accesstype.equalsIgnoreCase("Bitmap"))
            Access_status = 0;

        String OutAc_types = "";
        for (int i = 0; i < outercolumnindexs.length; i++) {
            if (Access_status != -1)
                OutAc_types = OutAc_types + get_accesstypes(OuterfileName, outercolumnindexs[i], Access_status);
            else
                OutAc_types = OutAc_types + "Columnscan";

            if (i!=((outercolumnindexs.length)-1))
                OutAc_types = OutAc_types + ",";
        }

        String[] innerColName = colNames(rightFilter);
        int[] innercolumnindexs = innercfmeta.getColindexes(innerColName);
        String InnerAc_types = "";
        for (int i = 0; i < innercolumnindexs.length; i++) {

            InnerAc_types = InnerAc_types + get_accesstypes(InnerfileName, innercolumnindexs[i], 0);
            if (i!=((innercolumnindexs.length)-1))
                InnerAc_types = InnerAc_types + ",";
        }
        String Out_Arg=Dbname+" "+OuterfileName+" "+outerColName[0]+" "+outFilter+" "+"500"+" "+OutAc_types+" "+"1"+" "+"1";
        String Inner_Arg=Dbname+" "+InnerfileName+" "+innerColName[0]+" "+rightFilter+" "+"500"+" "+InnerAc_types+" "+"-1"+" "+"1";

        ColumnarIndexScan Innercolscan = new tests.ColumnarIndexScan();
        Innercolscan.main(Inner_Arg.split(" "));

        ColumnarIndexScan Outercolscan = new tests.ColumnarIndexScan();
        Outercolscan.main(Out_Arg.split(" "));
        char[] AO = JoinFilter.toCharArray();
        ArrayList<Integer> op_index= new ArrayList<Integer>();
        for (int i = 0; i < JoinFilter.length(); i++) {
            if(AO[i]=='&'){
                op_index.add(1);
            }
            if(AO[i]=='|'){
                op_index.add(0);
            }
        }
        String[] Joinquery = JoinFilter.split("\\||&");
        String[] Joinvalue1 = new String[Joinquery.length];
        String[] Joinoperator = new String[Joinquery.length];
        String[] Joinvalue2 = new String[Joinquery.length];

        int keyType = 0;

        for (int j = 0; j <  Joinquery.length; j++) {
            int flag = 0;

            char[] VALUECONSTRAINT =  Joinquery[j].toCharArray();
            {
                for (int i = 0; i < VALUECONSTRAINT.length; i++) {
                    if (VALUECONSTRAINT[i] == '>' || VALUECONSTRAINT[i] == '<' || VALUECONSTRAINT[i] == '!' || VALUECONSTRAINT[i] == '=') {
                        if(Joinoperator[j]==null)
                            Joinoperator[j]=""+VALUECONSTRAINT[i];
                        else
                            Joinoperator[j] =  Joinoperator[j] + VALUECONSTRAINT[i];
                        flag = 1;

                    } else if (flag == 0) {
                        if( Joinvalue1[j]==null)
                            Joinvalue1[j]=""+VALUECONSTRAINT[i];
                        else
                            Joinvalue1[j] =  Joinvalue1[j] + VALUECONSTRAINT[i];
                    } else if (flag == 1) {
                        if( Joinvalue2[j]==null)
                            Joinvalue2[j]=""+VALUECONSTRAINT[i];
                        else
                            Joinvalue2[j] =  Joinvalue2[j] + VALUECONSTRAINT[i];

                    }
                }

            }
        }

        int[] join_out_indexes=outercfmeta.getColindexes(Joinvalue1);
        int[] join_in_indexes=outercfmeta.getColindexes(Joinvalue2);

        Heapfile Out_file = new Heapfile("NJP_outerJoin");

        RID rid1=new RID();
        Scan hScan1=Out_file.openScan();
        Tuple hTuple1=null;

        while((hTuple1=hScan1.getNext(rid1))!=null) {

            List<Integer>  New_op_index = new ArrayList<>(op_index);
            ArrayList<ArrayList<Integer>> Nlp_Positions = new ArrayList<ArrayList<Integer>>();

            int Out_Value = Convert.getIntValue(0, hTuple1.getTupleByteArray());


            Heapfile In_file = new Heapfile("NJP_InnerJoin");
            RID rid2 = new RID();
            Scan hScan2 = In_file.openScan();
            Tuple hTuple2 = null;
            ArrayList<Integer> In_Value = new ArrayList<Integer>();
            while ((hTuple2 = hScan2.getNext(rid1)) != null) {

                In_Value.add(Convert.getIntValue(0, hTuple2.getTupleByteArray()));

            }

            for(int i=0;i<join_out_indexes.length;i++) {
                AttrType attr = null;
                int I_O_V = 0;
                String S_O_V = null;
                RID out_rid = new RID();
                Heapfile out_hf = new Heapfile(outercfmeta.getHffile(join_out_indexes[i]));
                out_rid = out_hf.getrid(Out_Value);
                attr = (outercfmeta.getAttrTypes())[join_out_indexes[i]];
                if (attr.attrType == AttrType.attrInteger) {
                    byte[] data;
                    data = out_hf.getRecord(out_rid).getTupleByteArray();
                    I_O_V = (Convert.getIntValue(0, data));


                } else if (attr.attrType == AttrType.attrString) {
                    byte[] data;

                    data = out_hf.getRecord(out_rid).getTupleByteArray();
                    S_O_V = Convert.getStrValue(0, data, globalVar.sizeOfStr);

                }
            //    System.out.println(I_O_V);

                Heapfile In_hf = new Heapfile(innercfmeta.getHffile(join_in_indexes[i]));
                attr = (innercfmeta.getAttrTypes())[join_in_indexes[i]];
                int I_in_V = 0;
                String S_in_V = null;
                RID in_rid = new RID();
                ArrayList<Integer> temp_in_pos = new ArrayList<Integer>();
                for (int in = 0; in < In_Value.size(); in++) {
             //       System.out.println(In_Value.get(in));
                    in_rid = In_hf.getrid(In_Value.get(in));
                    if (attr.attrType == AttrType.attrInteger) {
                        byte[] data;

                        data = In_hf.getRecord(in_rid).getTupleByteArray();
                        I_in_V = (Convert.getIntValue(0, data));

                        if (Joinoperator[i].toCharArray()[0] == '<') {
                            if (I_in_V > I_O_V)
                                temp_in_pos.add(In_Value.get(in));

                        }
                        if (Joinoperator[i].toCharArray()[0] == '>') {
                            if (I_in_V < I_O_V)
                                temp_in_pos.add(In_Value.get(in));

                        }
                        if (Joinoperator[i].toCharArray()[0] == '=') {
                            if (I_in_V == I_O_V)
                                temp_in_pos.add(In_Value.get(in));

                        }
                        if (Joinoperator[i].toCharArray()[0] == '!') {
                            if (I_in_V != I_O_V)
                                temp_in_pos.add(In_Value.get(in));

                        }


                    } else if (attr.attrType == AttrType.attrString) {
                        byte[] data;

                        data = In_hf.getRecord(in_rid).getTupleByteArray();
                        S_in_V = Convert.getStrValue(0, data, globalVar.sizeOfStr);
                        if (Joinoperator[i].toCharArray()[0] == '<') {
                            if (S_in_V.compareTo(S_O_V) > 0)
                                temp_in_pos.add(In_Value.get(in));

                        }
                        if (Joinoperator[i].toCharArray()[0] == '>') {
                            if (S_in_V.compareTo(S_O_V) < 0)
                                temp_in_pos.add(In_Value.get(in));

                        }
                        if (Joinoperator[i].toCharArray()[0] == '=') {
                            if (S_in_V.compareTo(S_O_V) == 0)
                                temp_in_pos.add(In_Value.get(in));

                        }
                        if (Joinoperator[i].toCharArray()[0] == '!') {
                            if (S_in_V.compareTo(S_O_V) != 0)
                                temp_in_pos.add(In_Value.get(in));

                        }

                    }
                }
              //  System.out.println(temp_in_pos);
                Nlp_Positions.add(temp_in_pos);

            }
            for (int i = 0; i < New_op_index.size(); i++) {
                if (op_index.get(i) == 1) {

                    (Nlp_Positions.get(i)).retainAll(Nlp_Positions.get(i+1));
                    Nlp_Positions.set(i,Nlp_Positions.get(i));
                    Nlp_Positions.remove(i+1);
                    New_op_index.remove(i);
                    i=i-1;

                }
                }
            for (int i = 0; i < New_op_index.size(); i++) {
                if (op_index.get(i) == 0) {
                    (Nlp_Positions.get(i)).removeAll(Nlp_Positions.get(i+1));
                    (Nlp_Positions.get(i)).addAll(Nlp_Positions.get(i+1));

                    Nlp_Positions.set(i,Nlp_Positions.get(i));
                    Nlp_Positions.remove(i+1);
                    New_op_index.remove(i);
                    i=i-1;
                }
            }
            printCorrespondTuples(outercolindex,innercolindex, outerattrTarget, innerattrTarget, outercfmeta,innercfmeta,Nlp_Positions.get(0),Out_Value);
            }


    }
    public static void printCorrespondTuples(int[] outercolindexes,int[] incolindexes, AttrType[] outerattrTarget, AttrType[] innerattrTarget,ColumnDataPageInfo outcfmeta,ColumnDataPageInfo incfmeta,ArrayList<Integer> posrid,int Outposition) throws InvalidSlotNumberException, Exception {

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

            int offset = 0;
            for (int key : posrid) {
                System.out.print("[");

                System.out.print(ot);
                byte[] data;

                for (int i = 0; i < innerattrTarget.length; i++) {
                    Heapfile hf = new Heapfile(incfmeta.getHffile(incolindexes[i]));

                    RID rid = hf.getrid(key);
                    data = hf.getRecord(rid).getTupleByteArray();
                    if (innerattrTarget[i].attrType == AttrType.attrInteger) {

                        System.out.print(Convert.getIntValue(offset, data) + ",");

                    } else if (innerattrTarget[i].attrType == AttrType.attrString) {

                        System.out.print(Convert.getStrValue(offset, data, globalVar.sizeOfStr).trim() + ",");

                    }

                }
                System.out.print("]\n");

            }
}
}
