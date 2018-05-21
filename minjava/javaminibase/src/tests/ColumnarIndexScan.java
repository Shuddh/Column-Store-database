package tests;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.List;

import bitmap.*;
import btree.*;
import bufmgr.*;
import global.*;
import heap.*;
import columnar.*;
import diskmgr.*;
import tests.*;

public class ColumnarIndexScan {



    public BTreeFile btf;

    public void main(String[] args) throws Exception {

        int startread = pcounter.rcounter;
        int startwrite = pcounter.wcounter;
        String COLUMNDBNAME = args[0];
        String COLUMNARFILENAME = args[1];
        String[] TARGETCOLUMNNAMES = args[2].split(",");

        char[] AO = args[3].toCharArray();
        ArrayList<Integer> op_index= new ArrayList<Integer>();
        for (int i = 0; i < AO.length; i++) {
            if(AO[i]=='&'){
                op_index.add(1);
            }
            if(AO[i]=='|'){
                op_index.add(0);

            }
        }



        String[] query = args[3].split("\\||&");



        int NUMBUF = Integer.parseInt(args[4]);


        String[] ACCESSTYPE = args[5].split(",");

        int Join_Type=Integer.parseInt(args[6]);

        TID tid = new TID();
        int num_pages = 500;
        SystemDefs.JavabaseDB.openDB(COLUMNDBNAME);

        ColumnDataPageInfo meta = new ColumnDataPageInfo();
        ColumnDataPageInfo cfmeta = meta.ColumnDataPageInfo(COLUMNARFILENAME + ".hdr");


        int[] TargetColumnIndexes = cfmeta.getColindexes(TARGETCOLUMNNAMES);
        AttrType attrTarget[];
        attrTarget = cfmeta.getcolAttrTypes(TargetColumnIndexes);

        String[] ColName = new String[query.length];
        String[] operator = new String[query.length];
        String[] value = new String[query.length];

        int keyType = 0;

        for (int j = 0; j < query.length; j++) {
            int flag = 0;

            char[] VALUECONSTRAINT = query[j].toCharArray();
            {
                for (int i = 0; i < VALUECONSTRAINT.length; i++) {
                    if (VALUECONSTRAINT[i] == '>' || VALUECONSTRAINT[i] == '<' || VALUECONSTRAINT[i] == '!' || VALUECONSTRAINT[i] == '=') {
                        if(operator[j]==null)
                            operator[j]=""+VALUECONSTRAINT[i];
                        else
                            operator[j] = operator[j] + VALUECONSTRAINT[i];
                        flag = 1;

                    } else if (flag == 0) {
                        if(ColName[j]==null)
                            ColName[j]=""+VALUECONSTRAINT[i];
                        else
                            ColName[j] = ColName[j] + VALUECONSTRAINT[i];
                    } else if (flag == 1) {
                        if(value[j]==null)
                            value[j]=""+VALUECONSTRAINT[i];
                        else
                            value[j] = value[j] + VALUECONSTRAINT[i];

                    }
                }

            }
        }

        AttrType InputattrTypes[];
        // short targetsizes;
        int[] condcolumnindexs = cfmeta.getColindexes(ColName);
        InputattrTypes = cfmeta.getcolAttrTypes(condcolumnindexs);
        Columnarfile tempcf = new Columnarfile();
        Columnarfile cf = tempcf.getColumnarFile(cfmeta);

        AttrType[] AllColAttr = cfmeta.getAttrTypes();

        // ArrayList<ArrayList<Positions>>get_positions=new ArrayList<ArrayList<Positions>>();
        ArrayList<HashMap<Integer, ArrayList<RID>>> get_positions = new ArrayList<HashMap<Integer, ArrayList<RID>>>();
        ArrayList<Integer> targetridindex = new ArrayList<Integer>();



        for (int i = 0; i < ACCESSTYPE.length; i++) {

            //   ArrayList<>positions=new ArrayList<Positions>();

            HashMap<Integer, ArrayList<RID>> positions = new HashMap<>();


            if (ACCESSTYPE[i].equalsIgnoreCase("Btree")) {

                KeyDataEntry entry;

                if (InputattrTypes[i].attrType == AttrType.attrInteger) { //Assuming value is an Integer type
                    keyType = AttrType.attrInteger;
                    btf = new BTreeFile("BT!" + COLUMNARFILENAME + '!' + condcolumnindexs[i]);
                } else if (InputattrTypes[i].attrType == AttrType.attrString) { //Assuming value is a String type
                    keyType = AttrType.attrString;
                    btf = new BTreeFile("BT!" + COLUMNARFILENAME + '!' + condcolumnindexs[i]);
                }
                BTFileScan scan = btf.new_scan(null, null);


                while ((entry = scan.get_next()) != null) {

                    int eval = 0;
                    if (InputattrTypes[i].attrType == AttrType.attrInteger) {
                        int FieldValue = Integer.parseInt(entry.key.toString());


                        char[] op = operator[i].toCharArray();
                        for (int l = 0; l < operator[i].length(); l++) {
                            eval = 0;
                            if (op[l] == '<') {
                                if (Integer.parseInt(value[i]) > FieldValue)
                                    eval = 1;
                                else
                                    eval = 0;

                            }
                            if (op[l] == '>') {
                                if (Integer.parseInt(value[i]) < FieldValue)
                                    eval = 1;
                                else
                                    eval = 0;
                            }
                            if (op[l] == '=') {
                                if (Integer.parseInt(value[i]) == FieldValue)
                                    eval = 1;
                                else
                                    eval = 0;

                            }
                            if (op[l] == '!') {
                                if (Integer.parseInt(value[i]) != FieldValue)
                                    eval = 1;
                                else
                                    eval = 0;

                                l++;
                            }
                            if (eval == 1) {
                                RID tempRid = new RID();
                                tempRid = ((btree.LeafData) entry.data).getData();
                                if (InputattrTypes[i].attrType == AttrType.attrInteger) {
                                    Heapfile hf = new Heapfile(COLUMNARFILENAME + "." + condcolumnindexs[i]);
                                    //   positions.add(hf.getposition(tempRid));
                                    //       Positions position=new Positions();


                                    int hf_pos = hf.getposition(tempRid);


                                    Set<Integer> pos_keys = positions.keySet();
                                    ArrayList<RID> rids = new ArrayList<RID>();
                                    rids.add(tempRid);
                                    positions.put(hf_pos, rids);


                                    //         positions.add(position);
                                    //   printCorrespondTuples(columnindexes,attrTarget,AllColAttr,cfmeta,position);
                                }

                                eval = 0;
                            }
                        }

                    } else if (InputattrTypes[i].attrType == AttrType.attrString) {
                        String FieldValue = entry.key.toString();

                        char[] op = operator[i].toCharArray();
                        for (int l = 0; l < operator[i].length(); l++) {
                            eval = 0;
                            if (op[l] == '<') {
                                if (value[i].compareTo(FieldValue) > 0)
                                    eval = 1;
                                else
                                    eval = 0;
                            }
                            if (op[l] == '>') {
                                if (value[i].compareTo(FieldValue) < 0)
                                    eval = 1;
                                else
                                    eval = 0;
                            }
                            if (op[l] == '=') {
                                if (value[i].compareTo(FieldValue) == 0)
                                    eval = 1;
                                else
                                    eval = 0;

                            }
                            if (op[l] == '!') {
                                if (value[i].compareTo(FieldValue) != 0)
                                    eval = 1;
                                else
                                    eval = 0;
                                l++;
                            }
                            if (eval == 1) {
                                RID tempRid = new RID();
                                tempRid = ((btree.LeafData) entry.data).getData();
                                if (InputattrTypes[i].attrType == AttrType.attrString) {
                                    Heapfile hf = new Heapfile(COLUMNARFILENAME + "." + condcolumnindexs[i]);

                                    int hf_pos = hf.getposition(tempRid);
                                    Set<Integer> pos_keys = positions.keySet();
                                    ArrayList<RID> rids = new ArrayList<RID>();
                                    rids.add(tempRid);
                                    positions.put(hf_pos, rids);

                                }

                                eval = 0;
                            }
                        }
                    }


                }
                targetridindex.add(condcolumnindexs[i]);
                System.out.println("AT THE END OF SCAN!");
                get_positions.add(positions);


            }


            if (ACCESSTYPE[i].equalsIgnoreCase("Bitmap")) {


                RID rid=new RID();
                Heapfile Bitmap_metafile = new Heapfile(COLUMNARFILENAME+"Bitmap"+condcolumnindexs[i]);
                Scan hfscan =Bitmap_metafile.openScan();

                Tuple t=null;

                    while((t=hfscan.getNext(rid))!=null) {
                        int eval = 0;
                        String FieldValue = Convert.getStrValue(0,t.getTupleByteArray(), globalVar.sizeOfStr);

                        char[] op = operator[i].toCharArray();
                        for(int l = 0;l<op.length;l++)
                        {
                            eval =0;

                                if (op[l] == '<') {
                                    if (value[i].compareTo(FieldValue) > 0)
                                        eval = 1;
                                    else
                                        eval = 0;
                                }
                                if (op[l] == '>') {
                                    if (value[i].compareTo(FieldValue) < 0)
                                        eval = 1;
                                    else
                                        eval = 0;
                                }
                                if (op[l] == '=') {
                                    if (value[i].compareTo(FieldValue) == 0)
                                        eval = 1;
                                    else
                                        eval = 0;

                                }
                                if (op[l] == '!') {
                                    if (value[i].compareTo(FieldValue) != 0)
                                        eval = 1;
                                    else
                                        eval = 0;
                                    l++;
                                }

                            if(eval ==1){
                                try {
                                    String bmfName = "BM!" + COLUMNARFILENAME + '!' + condcolumnindexs[i] + '!' + FieldValue;
                                    BitMapFile bmf = new BitMapFile(bmfName);
                                    positions.putAll(bmf.getHashPositions(bmfName));


                                } catch (Exception e) {
                                    // TODO Auto-generated catch block
                                    //      e.printStackTrace();
                                }

                            }

                    }
                }

                get_positions.add(positions);

            }
            if(ACCESSTYPE[i].equalsIgnoreCase("Columnscan"))
            {

                int ind = condcolumnindexs[i];
                RID rid=new RID();
                Heapfile hf = new Heapfile(cfmeta.getHffile(condcolumnindexs[i]));
                Scan columnFileScan =hf.openScan();
                Tuple t=null;
                int position = 0;
                try
                {
                    while((t=columnFileScan.getNext(rid))!=null) {
                        position++;
                        //   System.out.println("ENTERED");
                        //  System.out.println(new String(t.getTupleByteArray()));
                        int eval =0;
                        if(InputattrTypes[i].attrType == AttrType.attrInteger)
                        {
                            int FieldValue = Convert.getIntValue(0,t.getTupleByteArray());
                            char[] op = operator[i].toCharArray();
                            for(int l = 0;l<operator[i].length();l++)
                            {
                                eval =0;
                                if(op[l] == '<')
                                {
                                    if(Integer.parseInt(value[i])>FieldValue)
                                        eval=1;
                                    else
                                        eval=0;

                                }
                                if (op[l] == '>')
                                {  if(Integer.parseInt(value[i])<FieldValue)
                                    eval=1;
                                else
                                    eval=0;
                                }
                                if (op[l] == '=')
                                {
                                    if(Integer.parseInt(value[i])==FieldValue)
                                        eval=1;
                                    else
                                        eval=0;

                                }
                                if(op[l] == '!')
                                {
                                    if(Integer.parseInt(value[i])!=FieldValue)
                                        eval=1;
                                    else
                                        eval=0;

                                    l++;
                                }
                                if(eval ==1) {
                                    ArrayList<RID> rids = null;
                                    positions.put(position, rids);
                                }
                                   // printCorrespondTuples(columnindexes,attrTarget,AllColAttr,cfmeta,position);
                            }

                        }
                        else if(InputattrTypes[i].attrType == AttrType.attrString)
                        {
                            String FieldValue = Convert.getStrValue(0,t.getTupleByteArray(), globalVar.sizeOfStr);

                            char[] op = operator[i].toCharArray();
                            for(int l = 0;l<op.length;l++)
                            {
                                eval =0;
                                if(op[l] == '<')
                                {
                                    if(value[i].compareTo(FieldValue)>0)
                                        eval=1;
                                    else
                                        eval=0;
                                }
                                if (op[l] == '>')
                                {  if(value[i].compareTo(FieldValue)<0)
                                    eval=1;
                                else
                                    eval=0;
                                }
                                if (op[l] == '=')
                                {
                                    if(value[i].compareTo(FieldValue)==0)
                                        eval=1;
                                    else
                                        eval=0;

                                }
                                if(op[l] == '!')
                                {
                                    if(value[i].compareTo(FieldValue)!=0)
                                        eval=1;
                                    else
                                        eval=0;
                                    l++;
                                }
                                if(eval ==1)
                                {
                                    ArrayList<RID> rids = null;
                                    positions.put(position, rids);

                                }
                                    //printCorrespondTuples(columnindexes,attrTarget,AllColAttr,cfmeta,position);
                            }
                        }


                    }
                    columnFileScan.closescan();
//System.out.println("Disk Reads"+ (pcounter.rcounter - startread));
//System.out.println("Disk Writes"+ (pcounter.wcounter - startwrite));

                }
                catch (Exception e)
                {
// TODO Auto-generated catch block
//e.printStackTrace();
                }

            }


        }

        for(int i=0;i<op_index.size();i++){
            if(op_index.get(i)==1) {

                ((get_positions.get(i)).keySet()).retainAll((get_positions.get(i+1)).keySet());
                Set<Integer> keys = get_positions.get(i).keySet();

                HashMap<Integer, ArrayList<RID>> temp = new HashMap<>();
                for (int key : keys) {

                    ArrayList<RID> ridslist = new ArrayList<RID>();

                    if ((get_positions.get(i)).get(key) != null)
                        ridslist.addAll((get_positions.get(i)).get(key));
                    if ((get_positions.get(i+1)).get(key) != null)
                        ridslist.addAll((get_positions.get(i+1)).get(key));

                    temp.put(key, ridslist);


                }
                //get_positions.remove(i+1);
               // get_positions.remove(i);
                get_positions.set(i+1,temp);
                get_positions.remove(i);
                op_index.remove(i);
                i=i-1;
            }
        }
        for(int i=0;i<op_index.size();i++){
            if(op_index.get(i)==0) {


                int flag=0;

                Set<Integer> keys = new HashSet<Integer>();
                Set<Integer> keys1 = get_positions.get(i).keySet();
               Set<Integer> keys2 = get_positions.get(i+1).keySet();
               if(keys1.size()<=keys2.size()) {
                   keys.addAll(keys1);
                   keys.addAll(keys2);
               }
               else{
                   keys.addAll(keys1);
                   keys.addAll(keys2);
                }

                HashMap<Integer, ArrayList<RID>> temp = new HashMap<>();
                for (int key : keys) {

                    ArrayList<RID> ridslist = new ArrayList<RID>();
                    if ((get_positions.get(i)).get(key) != null)
                        ridslist.addAll((get_positions.get(i)).get(key));
                    if ((get_positions.get(i+1)).get(key) != null)
                        ridslist.addAll((get_positions.get(i+1)).get(key));

                    temp.put(key, ridslist);


                }

                get_positions.set(i+1,temp);
                get_positions.remove(i);
                op_index.remove(i);
                i=i-1;



           }
        }

        printCorrespondTuples(TargetColumnIndexes, attrTarget, cfmeta, get_positions.get(0), targetridindex,Join_Type);

    }

    public static void printCorrespondTuples(int[] columnindexes, AttrType[] attrTarget, ColumnDataPageInfo cfmeta, HashMap<Integer, ArrayList<RID>> posrid, ArrayList<Integer> targetridindex,int Join_Type ) throws InvalidSlotNumberException, Exception {

        Set<Integer> keys = posrid.keySet();
        if(Join_Type==-1){

            Heapfile hf = new Heapfile("NJP_InnerJoin");
            if(SystemDefs.JavabaseDB.get_file_entry("NJP_InnerJoin") != null){
                hf.deleteFile();
                hf = new Heapfile("NJP_InnerJoin");
            }

            for(int key:keys)
            {
                byte[] Data=new byte[globalVar.sizeOfInt];
                Convert.setIntValue(key,0,Data);
                hf.insertRecord(Data);

            }



            }
        else if(Join_Type == 1)
        {
            Heapfile hf = new Heapfile("NJP_outerJoin");
            if(SystemDefs.JavabaseDB.get_file_entry("NJP_outerJoin") != null){
                hf.deleteFile();
                hf = new Heapfile("NJP_outerJoin");
            }

            for(int key:keys)
            {
                byte[] Data=new byte[globalVar.sizeOfInt];
                Convert.setIntValue(key,0,Data);
                hf.insertRecord(Data);
            }

        }
        else {

            System.out.println(keys);

            int offset = 0;
            for (int key : keys) {
                byte[] data;
                System.out.print("[");
                for (int i = 0; i < attrTarget.length; i++) {
                    Heapfile hf = new Heapfile(cfmeta.getHffile(columnindexes[i]));

                    RID rid = hf.getrid(key);
                    data = hf.getRecord(rid).getTupleByteArray();
                    if (attrTarget[i].attrType == AttrType.attrInteger) {

                        System.out.print(Convert.getIntValue(offset, data) + ",");

                    } else if (attrTarget[i].attrType == AttrType.attrString) {

                        System.out.print(Convert.getStrValue(offset, data, globalVar.sizeOfStr).trim() + ",");

                    }

                }
                System.out.print("]\n");

            }
        }

    }
}
