package columnar;

import heap.*;
import global.*;

public class TupleScan {

    private Scan[] heapfileScan;
    private Columnarfile currentfile;

    public TupleScan(Columnarfile cf) throws InvalidTupleSizeException
    {
        currentfile = cf;

        //initialize the scan to number of columns in the table
        heapfileScan = new Scan[cf.getNumColumns()];
        try {
            //hfColumns is the array of type heapfile that contains all the columns
            for(int i=1; i<cf.getNumColumns();i++)
            {
                heapfileScan[i] = cf.hfColumns[i].openScan();
            }

        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void closetuplescan()
    {
        try {
            for (int i = 1; i < currentfile.getNumColumns(); i++) {
                heapfileScan[i].closescan();
            }

            heapfileScan = null;
            currentfile = null;
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public Tuple getNext (TID tid) {
        int currOffset = 0;

        Tuple next = new Tuple(currentfile.getTupleLength());

        //use scan.getNext function on each RIDs to get the next tuple of every column
        try{

            short[] fldoffset = new short[currentfile.getTupleCnt()];
            int j =0;
            for (Scan hpfl : heapfileScan )
            {
                if(currentfile.type[j].attrType == AttrType.attrInteger)
                {
                    //get the next tuple
                    Tuple temp = hpfl.getNext((tid.recordIDs[j]));

                    if (temp == null)
                        return null;

                    //set the value of corresponding field in the data to next tuple value
                    next.setIntFld(j+1, temp.getIntFld(1));
                }
                if(currentfile.type[j].attrType == AttrType.attrString)
                {
                    Tuple temp = hpfl.getNext(tid.recordIDs[j]);

                    if(temp == null)
                        return null;

                    next.setStrFld(j+1, temp.getStrFld(1));
                }
                j++;
            }
            // set tuple_length, fldcnt, fldoff fields of next tuple
            for (int i = 0; i < currentfile.getNumColumns(); i++)
            {
                if (currentfile.type[i].attrType == AttrType.attrString)
                {
                    fldoffset[i] = (short) currOffset;
                    currOffset = currOffset + Size.STRINGSIZE;
                }
                if(currentfile.type[i].attrType == AttrType.attrInteger)
                {
                    fldoffset[i] = (short) currOffset;
                    currOffset = currOffset + INTSIZE;
                }
            }
            next.setTupleValues(currentfile.getTupleLength(), (short)currentfile.getNumColumns(),fldoffset);

        }catch (Exception e)
        {
            e.printStackTrace();
        }
        return next;
    }

    public boolean position (TID tid)
    {
       // int i = 0;
        try {
            //for (Scan hpfl : heapfileScan)
            for(int i=1; i<heapfileScan.length;i++)
            {
                if(!heapfileScan[i].position(tid.recordIDs[i++]))
                    return false;
            }
            return true;

        }catch (Exception e)
        {
            e.printStackTrace();
        }
        return  false;
    }
}
