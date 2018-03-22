package columnar;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import heap.*;
import global.*;

import java.io.IOException;

public class TupleScan {

    private Scan[] hfScans;
    private Columnarfile currFile;

    public TupleScan(Columnarfile cf) throws InvalidTupleSizeException {
        this.currFile = cf;
        //initialize the scan to number of columns in the table
        hfScans = new Scan[cf.getNumColumns()];
        try {
            //hfColumns is the array of type heapfile that contains all the columns
            for (int i = 0; i < cf.getNumColumns(); i++)
                hfScans[i] = cf.hfColumns[i].openScan();
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

    public Tuple getNext(TID tid) throws InvalidTupleSizeException, FieldNumberOutOfBoundException, IOException {
        Tuple nextTup = new Tuple(currFile.getTupleLength());
        Tuple temp;
        for (int i = 0; i < currFile.getNumColumns(); i++) {
            if (currFile.type[i].attrType == AttrType.attrString) {
                temp = hfScans[i].getNext(tid.recordIDs[i]);
                nextTup.setStrFld(i + 1, temp.getStrFld(1));
            }
            if (currFile.type[i].attrType == AttrType.attrInteger) {
                temp = hfScans[i].getNext(tid.recordIDs[i]);
                nextTup.setIntFld(i + 1, temp.getIntFld(1));
            }
            if (currFile.type[i].attrType == AttrType.attrReal) {
                temp = hfScans[i].getNext(tid.recordIDs[i]);
                nextTup.setFloFld(i + 1, temp.getFloFld(1));
            }
        }
        return nextTup;
    }

    public boolean position(TID tid) throws InvalidTupleSizeException, IOException {
        boolean status = false;
        for (int i = 0; i < hfScans.length; i++)
            status = hfScans[i].position(tid.recordIDs[i]);
        return status;
    }
}