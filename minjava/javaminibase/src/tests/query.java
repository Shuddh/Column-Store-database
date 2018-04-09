package tests;




import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import bitmap.*;
import btree.KeyClass;
import btree.*;
import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.IteratorException;
import btree.KeyNotMatchException;
import btree.PinPageException;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.*;
import heap.*;
import columnar.*;
import diskmgr.*;

public class query {

	public BTreeFile btf;
	
    public void main(String[] args) throws InvalidSlotNumberException, Exception {


    	int startread = pcounter.rcounter;
        int startwrite = pcounter.wcounter;
        String COLUMNDBNAME = args[0];
        String COLUMNARFILENAME = args[1];
        String[] TARGETCOLUMNNAMES = args[2].split(",");
        String query = args[3];
        int NUMBUF = Integer.parseInt(args[4]);
        String ACCESSTYPE = args[5];
        
        TID tid = new TID();
        int num_pages = 100000;
        SystemDefs.JavabaseDB.openDB(COLUMNDBNAME);
        
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
                int position=0;
                try {
                while ((t=tuplescan.getNext(tid1))!=null)
                {
                    position++;
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
                            if(eval ==1)

                                printSelectTuples(t,columnindexes,attrTarget,AllColAttr);
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
                            if(eval ==1)

                                printSelectTuples(t,columnindexes,attrTarget,AllColAttr);
                        }
                    }


                }
                }
                catch (NullPointerException e) {
            //        e.printStackTrace();
                }
                
           //     System.out.println("Disk Reads"+ (pcounter.rcounter-startread));
            //    System.out.println("Disk Writes"+ (pcounter.wcounter-startwrite));

           
        	}
      //  try {
  //          SystemDefs.JavabaseDB.closeDB();
  //      } catch (Exception e) {
  //          e.printStackTrace();
   //     }
    
     if(ACCESSTYPE.equalsIgnoreCase("Columnscan"))
    {
    	
        int ind = condcolumnindex;
        RID rid=new RID();
        Heapfile hf = cf.getHfColumns()[ind];
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
                   if(eval ==1)
                       printCorrespondTuples(columnindexes,attrTarget,AllColAttr,cfmeta,position);
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
                   if(eval ==1)
                       printCorrespondTuples(columnindexes,attrTarget,AllColAttr,cfmeta,position);
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

    if(ACCESSTYPE.equalsIgnoreCase("Btree"))
    {
    	
        KeyDataEntry entry;
        if(targetattr.attrType == AttrType.attrInteger) { //Assuming value is an Integer type
        	keyType=AttrType.attrInteger;
          btf = new BTreeFile("BT!"+COLUMNARFILENAME+'!'+condcolumnindex);}
        else if(targetattr.attrType == AttrType.attrString) { //Assuming value is a String type
        	keyType=AttrType.attrString;
        	btf = new BTreeFile("BT!"+COLUMNARFILENAME+'!'+condcolumnindex);}
     //   BT.printBTree(btf.getHeaderPage());
       // BT.printBTree(btf.getHeaderPage());
     //   System.out.println("printing leaf pages...");
     //   BT.printAllLeafPages(btf.getHeaderPage());
        BTFileScan scan = btf.new_scan(new IntegerKey(0),new IntegerKey(btf.MAX_SPACE));
        while((entry=scan.get_next())!=null)
        {
     //       System.out.println(entry.data.toString());

        	 int eval =0;
             if(targetattr.attrType == AttrType.attrInteger)
             {
             	int FieldValue = Integer.parseInt(entry.key.toString());

        //         System.out.println("FieldValue");
        //         System.out.println(FieldValue);

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
                     if(eval==1)
                     {
                         RID tempRid=new RID();
                         tempRid=((btree.LeafData)entry.data).getData();
                         if(targetattr.attrType == AttrType.attrInteger)
                         {
                             Heapfile hf = new Heapfile(COLUMNARFILENAME+"."+condcolumnindex);
                             int position=hf.getposition(tempRid);
                             printCorrespondTuples(columnindexes,attrTarget,AllColAttr,cfmeta,position);
                         }

                         eval=0;
                     }
                 }

             }
             else if(targetattr.attrType == AttrType.attrString)
             {
          	  String FieldValue = entry.key.toString();
             	
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
                     if(eval==1)
                     {
                         RID tempRid=new RID();
                         tempRid=((btree.LeafData)entry.data).getData();
                         if(targetattr.attrType == AttrType.attrInteger)
                         {
                             Heapfile hf = new Heapfile(COLUMNARFILENAME+"."+condcolumnindex);
                             int position=hf.getposition(tempRid);
                             printCorrespondTuples(columnindexes,attrTarget,AllColAttr,cfmeta,position);
                         }

                         eval=0;
                     }
                 }
             }

        


        } 
        System.out.println("AT THE END OF SCAN!");
    //    System.out.println("Disk Reads"+ (pcounter.rcounter - startread));
    //    System.out.println("Disk Writes"+ (pcounter.wcounter - startwrite));
    

        }
    

 if(ACCESSTYPE.equalsIgnoreCase("Bitmap"))
    {
        if(operator.equals("=")){
            try
            {
            	
                Heapfile hf = new Heapfile(COLUMNARFILENAME+"."+condcolumnindex);
               
                String bmfName="BM!"+COLUMNARFILENAME+'!'+condcolumnindex+'!'+value;
         //       System.out.println(bmfName);
                BitMapFile bmf = new BitMapFile(bmfName);
                int[] positions = bmf.getPositions(bmfName);
                for(int i=0;i<positions.length ;i++)
                {
                	if(positions[i]==0)
                	break;
                	printCorrespondTuples(columnindexes,attrTarget,AllColAttr,cfmeta,positions[i]);
                }
               
      //          System.out.println("Disk Reads"+ (pcounter.rcounter - startread));
      //          System.out.println("Disk Writes"+ (pcounter.wcounter - startwrite));
            } catch (Exception e)
            {
                // TODO Auto-generated catch block
          //      e.printStackTrace();
            }
        }

    }

}
  

        
public static void printCorrespondTuples(int[] columnindexes, AttrType[] attrTarget, AttrType[] AllColAttr,ColumnDataPageInfo cfmeta,int position) throws InvalidSlotNumberException, Exception
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
    		  if(columnindexes[j]==i) {
    			 
    			  Heapfile hf = new Heapfile(cfmeta.getHffile(columnindexes[j]));
    			  rid = hf.getrid(position);
    			  data = hf.getRecord(rid).getTupleByteArray();
    			
    			 System.out.print(Convert.getIntValue(offset,data)+",");j++;
    		  }	  
          }
          else  if(AllColAttr[i].attrType == AttrType.attrString)
          {
    		  if(columnindexes[j]==i) {
    			  Heapfile hf = new Heapfile(cfmeta.getHffile(columnindexes[j]));
    			  rid = hf.getrid(position);
    			  data = hf.getRecord(rid).getTupleByteArray();
    			
    			  System.out.print(Convert.getStrValue(offset,data,globalVar.sizeOfStr).trim()+",");j++;
    		  }
    			  
          }if(j==columnindexes.length)
        	  break;
    }
        System.out.print("]\n");
        }
public static void printSelectTuples(Tuple t,int[] columnindexes, AttrType[] attrTarget, AttrType[] AllColAttr) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidTupleSizeException
{
int offset =0;
byte[] data = t.getTupleByteArray();
System.out.print("[");
for(int i=0,j=0;i<AllColAttr.length;i++)
{
  if(AllColAttr[i].attrType == AttrType.attrInteger)
  {
	  if(columnindexes[j]==i) {
		 System.out.print(Convert.getIntValue(offset,data)+",");j++;
	  }	  
	  offset+=globalVar.sizeOfInt;
  }
  else  if(AllColAttr[i].attrType == AttrType.attrString)
  {
	  if(columnindexes[j]==i) {
		  
		  System.out.print(Convert.getStrValue(offset,data,globalVar.sizeOfStr).trim()+",");j++;
	  }
	  offset+=globalVar.sizeOfStr;
		  
  }if(j==columnindexes.length)
	  break;
}
System.out.print("]\n");
}

  

    }
