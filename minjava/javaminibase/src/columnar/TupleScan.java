package columnar;


import heap.*;
import global.*;

import java.io.IOException;
import java.util.Arrays;

public class TupleScan {

    private Scan[] hfScans;
    private Columnarfile currFile;
    private TID currtid =new TID();

    public TupleScan(Columnarfile cf) throws InvalidTupleSizeException {
        this.currFile = cf;
        //initialize the scan to number of columns in the table
        hfScans = new Scan[cf.getNumColumns()];
        try {
            //hfColumns is the array of type heapfile that contains all the columns
            for (int i = 0; i < cf.getNumColumns(); i++) {
                hfScans[i] = cf.hfColumns[i].openScan();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public TupleScan(Columnarfile cf,int ind) throws InvalidTupleSizeException {
        this.currFile = cf;
        //initialize the scan to number of columns in the table
        hfScans = new Scan[1];
        try {
            //hfColumns is the array of type heapfile that contains all the columns
          //  for (int i = 0; i < 1; i++) {
                hfScans[ind] = cf.hfColumns[ind].openScan();
          //  }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void closetuplescan() {
        for (int i = 0; i < hfScans.length; i++)
            hfScans[i].closescan();
        hfScans = null;
        currFile = null;
    }
    public TID getTid() {
    	return currtid;
    	
    }
    public Tuple getNext(TID tid) throws InvalidTupleSizeException, FieldNumberOutOfBoundException, IOException, InvalidTypeException {
        try {
       	 TID tid1= new TID();
       	 tid1.recordIDs = new RID[currFile.getNumColumns()];
       	 for(int j =0 ; j < currFile.getNumColumns() ; j++)
             tid1.recordIDs[j] = new RID();
           byte[] tupledata=new byte[currFile.getTupleLength()];
           int offset=0;
          for (int i = 0; i < currFile.getNumColumns(); i++) {
           	 Tuple temp = null;
               if (currFile.type[i].attrType == AttrType.attrString) {
                   temp = hfScans[i].getNext(tid.recordIDs[i]);
                   (tid1.recordIDs[i]).copyRid(tid.recordIDs[i]);
                   
                   String val = Convert.getStrValue(0, temp.getTupleByteArray(), 
                   		globalVar.sizeOfStr);
                   Convert.setStrValue(val,offset ,tupledata);
                   offset+=globalVar.sizeOfStr;
               }
               if (currFile.type[i].attrType == AttrType.attrInteger) {
               	temp = hfScans[i].getNext(tid.recordIDs[i]);
               	(tid1.recordIDs[i]).copyRid(tid.recordIDs[i]);
               	int val = Convert.getIntValue(0, temp.getTupleByteArray());
               	 Convert.setIntValue(val,offset ,tupledata);
                    offset+=globalVar.sizeOfInt;
               }
            }
         	tid1.numRIDs=1;
           	tid1.position=0;
           (this.currtid).copyTid(tid1);
           Tuple nextTup=new Tuple(tupledata,0,currFile.getTupleLength());
           return nextTup;}
           catch (NullPointerException e) {
             
           }
   	return null;
           
       }
  
    
    public boolean position(TID tid) throws InvalidTupleSizeException, IOException {
        boolean status = false;
        for (int i = 0; i < hfScans.length; i++)
            status = hfScans[i].position(tid.recordIDs[i]);
        return status;
    }
}