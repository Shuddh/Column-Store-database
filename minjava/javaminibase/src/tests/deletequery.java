package tests;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.StringTokenizer;

import bitmap.*;
import btree.*;
import global.*;
import heap.*;
import columnar.*;
import diskmgr.*;

public class deletequery {
	public BTreeFile btf;
	
    public void main(String[] args) throws InvalidSlotNumberException, Exception {
    	
        String COLUMNDBNAME = args[0];
        String COLUMNARFILENAME = args[1];
        String[] TARGETCOLUMNNAMES = args[2].split(",");
        String query = args[3];
        int NUMBUF = Integer.parseInt(args[4]);
        String ACCESSTYPE = args[5];
        int PURGE = Integer.parseInt(args[6]);
        TID tid = new TID();
        int num_pages = 10000;
        SystemDefs.JavabaseDB.openDB(COLUMNDBNAME);
        int startread = pcounter.rcounter;
        int startwrite = pcounter.wcounter;
        ColumnDataPageInfo meta = new ColumnDataPageInfo();
        ColumnDataPageInfo cfmeta = meta.ColumnDataPageInfo(COLUMNARFILENAME+".hdr");
        
   
        int[] columnindexes = cfmeta.getColindexes(TARGETCOLUMNNAMES);
        AttrType attrTarget[];
        attrTarget =cfmeta.getcolAttrTypes(columnindexes);

        String[] ColName= {""};
        String operator="";
        String value="";
        int flag=0;
        int keyType=0;
        char[] VALUECONSTRAINT= query.toCharArray();
        {
            for(int i=0; i<VALUECONSTRAINT.length ;i++)
            {
                if(VALUECONSTRAINT[i]=='>'||VALUECONSTRAINT[i]=='<'|| VALUECONSTRAINT[i]=='!'||VALUECONSTRAINT[i]=='=')
                {
                    operator=operator+VALUECONSTRAINT[i];
                    flag=1;

                }
                else if(flag==0)
                {
                    ColName[0]=ColName[0]+VALUECONSTRAINT[i];
                }
                else if(flag==1)
                {
                    value=value+VALUECONSTRAINT[i];
                }
            }

        }
        AttrType targetattr;
        short targetsizes;
        int condcolumnindex=cfmeta.getColindexes(ColName)[0];
        int[] temp =new int[1];
        temp[0]= condcolumnindex;
        targetattr= cfmeta.getcolAttrTypes(temp)[0];
        Columnarfile tempcf = new Columnarfile();
        Columnarfile cf = tempcf.getColumnarFile(cfmeta);
       
        AttrType[] AllColAttr = cfmeta.getAttrTypes();
        if(ACCESSTYPE.equalsIgnoreCase("FileScan"))
        {
        	  TupleScan tuplescan = new TupleScan(cf);
              TID tid1 = new TID();
              Tuple t = null;
              tid1.recordIDs = new RID[cf.getNumColumns()];
              for(int j =0 ; j < cf.getNumColumns() ; j++)
                  tid1.recordIDs[j] = new RID();
           //   System.out.println("hellooo");
              int position=0;
              try {
              while ((t=tuplescan.getNext(tid1))!=null)
              { 
           // 	  System.out.println("tiddd");
            	//  System.out.println(tid1.numRIDs);
                  position++;
                  TID currtid=new TID();
                  currtid.copyTid(tuplescan.getTid());
                  int eval=0; int offset=0;
                  
                  for(int i=0;i<condcolumnindex;i++)
                  {
                  	
                  	  if(AllColAttr[i].attrType == AttrType.attrInteger)
                        {
                        offset+=globalVar.sizeOfInt;
                        }
                        else if(AllColAttr[i].attrType == AttrType.attrString)
                        {
                        offset+=globalVar.sizeOfStr;
                        }
                  }
                 
                  if(targetattr.attrType == AttrType.attrInteger)
                  {
                  	int FieldValue = Convert.getIntValue(offset,t.getTupleByteArray());
                  	  char[] op = operator.toCharArray();
                      for(int l = 0;l<operator.length();l++)
                      {
                          eval =0;
                          if(op[l] == '<')
                          {
                              if(Integer.parseInt(value)>FieldValue)
                                  eval=1;
                              else
                                  eval=0;

                          }
                          if (op[l] == '>')
                          {  if(Integer.parseInt(value)<FieldValue)
                              eval=1;
                          else
                              eval=0;
                          }
                          if (op[l] == '=')
                          {
                              if(Integer.parseInt(value)==FieldValue)
                                  eval=1;
                              else
                                  eval=0;
                          }
                          if(op[l] == '!')
                          {
                              if(Integer.parseInt(value)!=FieldValue)
                                  eval=1;
                              else
                                  eval=0;

                              l++;
                          }
                          if(eval ==1) {

                              cf.markTupleDeleted(currtid,PURGE);}
                      }

                  }
                  else if(targetattr.attrType == AttrType.attrString)
                  {
                  	String FieldValue = Convert.getStrValue(offset,t.getTupleByteArray(),globalVar.sizeOfStr);
                  	
                  	char[] op = operator.toCharArray();
                      for(int l = 0;l<op.length;l++)
                      {
                          eval =0;
                          if(op[l] == '<')
                          {
                              if(value.compareTo(FieldValue)>0)
                                  eval=1;
                              else
                                  eval=0;
                          }
                          if (op[l] == '>')
                          {  if(value.compareTo(FieldValue)<0)
                              eval=1;
                          else
                              eval=0;
                          }
                          if (op[l] == '=')
                          {
                              if(value.compareTo(FieldValue)==0)
                                  eval=1;
                              else
                                  eval=0;

                          }
                          if(op[l] == '!')
                          {
                              if(value.compareTo(FieldValue)!=0)
                                  eval=1;
                              else
                                  eval=0;
                              l++;
                          }
                          if(eval ==1) {

                              cf.markTupleDeleted(currtid,PURGE);}
                      }
                  }

                    //      printSelectTuples(t,columnindexes,attrTarget,AllColAttr);

              }
                  if(PURGE ==1)
                  {
                      System.out.println("deleted records stored in markdeletefile");
                      cf.purgeAllDeletedTuples();
                      System.out.println("records deleted from markdeletefile");

                  }
                  else
                      System.out.println("records deleted permanently");

              }
              catch (NullPointerException e) {
        //          e.printStackTrace();
              }
              
   //           System.out.println("Disk Reads"+ (pcounter.rcounter-startread));
   //           System.out.println("Disk Writes"+ (pcounter.wcounter-startwrite));

         
      	}

                  
    if(ACCESSTYPE.equalsIgnoreCase("Columnscan"))
    {
        ArrayList<TID> tids=new ArrayList<TID>();
    	 
    	 int ind = condcolumnindex;
         RID rid=new RID();
         Heapfile hf = cf.getHfColumns()[ind];
         
         Scan columnFileScan = hf.openScan();
         Tuple t=null;
         int position = 0;
         try
         {
         while((t=columnFileScan.getNext(rid))!=null) {
        	
         position++;
            int eval =0;
            if(targetattr.attrType == AttrType.attrInteger)
            {
            	int FieldValue = Convert.getIntValue(0,t.getTupleByteArray());
            	  char[] op = operator.toCharArray();
                for(int l = 0;l<operator.length();l++)
                {
                    eval =0;
                    if(op[l] == '<')
                    {
                        if(Integer.parseInt(value)>FieldValue)
                            eval=1;
                        else
                            eval=0;

                    }
                    if (op[l] == '>')
                    {  if(Integer.parseInt(value)<FieldValue)
                        eval=1;
                    else
                        eval=0;
                    }
                    if (op[l] == '=')
                    {
                        if(Integer.parseInt(value)==FieldValue)
                            eval=1;
                        else
                            eval=0;

                    }
                    if(op[l] == '!')
                    {
                        if(Integer.parseInt(value)!=FieldValue)
                            eval=1;
                        else
                            eval=0;

                        l++;
                    }
                    if(eval ==1) {
                        tids.add(getCorrespondTuples(attrTarget,AllColAttr,cfmeta,position));}
                }

            }
            else if(targetattr.attrType == AttrType.attrString)
            {
         	  String FieldValue = Convert.getStrValue(0,t.getTupleByteArray(), globalVar.sizeOfStr);
            	
            	char[] op = operator.toCharArray();
                for(int l = 0;l<op.length;l++)
                {
                    eval =0;
                    if(op[l] == '<')
                    {
                        if(value.compareTo(FieldValue)>0)
                            eval=1;
                        else
                            eval=0;
                    }
                    if (op[l] == '>')
                    {  if(value.compareTo(FieldValue)<0)
                        eval=1;
                    else
                        eval=0;
                    }
                    if (op[l] == '=')
                    {
                        if(value.compareTo(FieldValue)==0)
                            eval=1;
                        else
                            eval=0;

                    }
                    if(op[l] == '!')
                    {
                        if(value.compareTo(FieldValue)!=0)
                            eval=1;
                        else
                            eval=0;
                        l++;
                    }
                    if(eval ==1) {
                        tids.add(getCorrespondTuples(attrTarget,AllColAttr,cfmeta,position));}
                }
            }



         }
         for(int i=0; i<tids.size();i++) {
             cf.markTupleDeleted(tids.get(i),PURGE);

         }

             if(PURGE ==1)
             {
                 System.out.println("deleted records stored in markdeletefile");
                 cf.purgeAllDeletedTuples();
                 System.out.println("records deleted from markdeletefile");

             }
             else
                 System.out.println("records deleted permanently");
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
    if(ACCESSTYPE.equalsIgnoreCase("Bitmap"))
    {
        if(operator.equals("==")){
            ArrayList<TID> tids=new ArrayList<TID>();
            try
            {
                Heapfile hf = new Heapfile(COLUMNARFILENAME+"."+condcolumnindex);
               
                String bmfName="BM!"+COLUMNARFILENAME+"!"+condcolumnindex+"!"+value;
             
                BitMapFile bmf = new BitMapFile(bmfName);
                int[] positions = bmf.getPositions(bmfName);

                for(int i=0;i<positions.length ;i++)
                {
            //        System.out.println("positions "+ positions[i]);
                	if(positions[i]==0)
                	break;

                    tids.add(getCorrespondTuples(attrTarget,AllColAttr,cfmeta,positions[i]));
                /*	 cf.markTupleDeleted(getCorrespondTuples(attrTarget,AllColAttr,cfmeta,positions[i]));
                	 System.out.println("deleted records stored in markdeletefile");
                	 if(PURGE ==1)
               	  {
               	  cf.purgeAllDeletedTuples();
                      System.out.println("records deleted from markdeletefile");
               	  } */

                }
           //     System.out.println("out");
                bmf.destroyBitMapFile(bmfName);

                for(int i=0; i<tids.size();i++) {
                    cf.markTupleDeleted(tids.get(i),PURGE);

                }

                //    for( int i=0;i<delTupleRIDs.size();i++) {
                //  }
                if(PURGE ==1)
                {
                    System.out.println("deleted records stored in markdeletefile");
                    cf.purgeAllDeletedTuples();
                    System.out.println("records deleted from markdeletefile");

                }
                else
                    System.out.println("records deleted permanently");
                System.out.println("AT THE END OF SCAN!");
               
     //           System.out.println("Disk Reads"+ (pcounter.rcounter - startread));
     //           System.out.println("Disk Writes"+ (pcounter.wcounter - startwrite));
            } catch (Exception e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    if(ACCESSTYPE.equalsIgnoreCase("Btree"))
    {
        ArrayList<RID> delTupleRIDs=new ArrayList<RID>();
        ArrayList<TID> tids=new ArrayList<TID>();
    	 KeyDataEntry entry;
    	 KeyClass delkey = null;
         if(targetattr.attrType == AttrType.attrInteger) { //Assuming value is an Integer type
         	keyType=AttrType.attrInteger;
           btf = new BTreeFile("BT!"+COLUMNARFILENAME+'!'+condcolumnindex);}
         else if(targetattr.attrType == AttrType.attrString) { //Assuming value is a String type
         	keyType=AttrType.attrString;
         	btf = new BTreeFile("BT!"+COLUMNARFILENAME+'!'+condcolumnindex);}
         BT.printBTree(btf.getHeaderPage());
     //    BTFileScan scan = btf.new_scan(new IntegerKey(0),new IntegerKey(btf.MAX_SPACE));
        BTFileScan scan = btf.new_scan(null,null);

         while((entry=scan.get_next())!=null)
         {
        //     System.out.println(entry.data.toString());
         	 int eval =0;
              if(targetattr.attrType == AttrType.attrInteger)
              {
              	int FieldValue = Integer.parseInt(entry.key.toString());
         //     	System.out.println("FieldValue");
         //         System.out.println(FieldValue);
         //         System.out.println(entry.data.toString());
              	  char[] op = operator.toCharArray();
                  for(int l = 0;l<operator.length();l++)
                  {
                      eval =0;
                      if(op[l] == '<')
                      {
                          if(Integer.parseInt(value)>FieldValue)
                              eval=1;
                          else
                              eval=0;

                      }
                      if (op[l] == '>')
                      {  if(Integer.parseInt(value)<FieldValue)
                          eval=1;
                      else
                          eval=0;
                      }
                      if (op[l] == '=')
                      {
                          if(Integer.parseInt(value)==FieldValue)
                              eval=1;
                          else
                              eval=0;

                      }
                      if(op[l] == '!')
                      {
                          if(Integer.parseInt(value)!=FieldValue)
                              eval=1;
                          else
                              eval=0;

                          l++;
                      }
                  }

              }


         
              if(eval==1)
             {
         //        System.out.println("evalenter");
                 RID tempRid=new RID();
                 tempRid=((btree.LeafData)entry.data).getData();
        //         System.out.println(entry.data.toString());
               //
                 //  System.out.println(tempRid.slotNo);
               //  delTupleRIDs.copyRid(tempRid);
                 delTupleRIDs.add(tempRid);
                 delkey = entry.key;

                 if(targetattr.attrType == AttrType.attrInteger)
                 {
                     Heapfile hf = new Heapfile(COLUMNARFILENAME+"."+condcolumnindex);
                     int position=hf.getposition(tempRid);
            //         System.out.println("position");
            //         System.out.println(position);
                     tids.add(getCorrespondTuples(attrTarget,AllColAttr,cfmeta,position));

                 }

                 eval=0;
             }
                        
         }
         for(int i=0;i<delTupleRIDs.size();i++){
            try{
            btf.Delete(delkey,delTupleRIDs.get(i));

            }
            catch (Exception e) {
         //       e.printStackTrace();
            }
         }
       //  }
        BT.printBTree(btf.getHeaderPage());
        System.out.println("printing leaf pages...");
        BT.printAllLeafPages(btf.getHeaderPage());

        for(int i=0; i<tids.size();i++) {
            cf.markTupleDeleted(tids.get(i),PURGE);

        }

   //    for( int i=0;i<delTupleRIDs.size();i++) {
     //  }
       if(PURGE ==1)
    	  {
              System.out.println("deleted records stored in markdeletefile");
    	  cf.purgeAllDeletedTuples();
              System.out.println("records deleted from markdeletefile");

          }
       else
           System.out.println("records deleted permanently");
         System.out.println("AT THE END OF SCAN!");
  //       System.out.println("Disk Reads"+ (pcounter.rcounter - startread));
  //       System.out.println("Disk Writes"+ (pcounter.wcounter - startwrite));
     

         }
     }

    public static TID getCorrespondTuples( AttrType[] attrTarget, AttrType[] AllColAttr,ColumnDataPageInfo cfmeta,int position) throws InvalidSlotNumberException, Exception
    {
 //       System.out.println("position1");

 //         System.out.println(position);
int offset =0;
RID rid = new RID();
byte[] data ;
System.out.print("[");
Tuple t;
TID tid=new TID();
tid.recordIDs=new RID[cfmeta.getNumColumns()];
tid.position=position;
tid.numRIDs=1;
for(int i=0;i<AllColAttr.length;i++)
{
	tid.recordIDs[i]=new RID();
	  if(AllColAttr[i].attrType == AttrType.attrInteger)
      {  	
			  Heapfile hf = new Heapfile(cfmeta.getHffile(i));
			  rid = hf.getrid(position);
			  data = hf.getRecord(rid).getTupleByteArray();
			
			  tid.recordIDs[i]=rid;
			
      }
      else  if(AllColAttr[i].attrType == AttrType.attrString)
      {

			  Heapfile hf = new Heapfile(cfmeta.getHffile(i));
			  rid = hf.getrid(position);
			  data = hf.getRecord(rid).getTupleByteArray();
			  tid.recordIDs[i]=rid;
			  
			 
		  
			  
      }
}
    System.out.print("]\n");
    return tid;
    }






        }