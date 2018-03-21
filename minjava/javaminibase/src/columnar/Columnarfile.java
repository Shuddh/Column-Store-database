package columnar;

import global.AttrType;
import global.*;
import global.GlobalConst;
import heap.*;

import java.io.IOException;

public class Columnarfile implements GlobalConst {
    private Heapfile columnarFile;
    private String filename;
    private int numColumns;
    AttrType[] type;
    public Heapfile[] hfColumns;
    public String[] hfNames;
    public int tupleLength;
    private ColumnDataPageInfo cdpinfo;


    public Columnarfile(String name, int numColumns, AttrType[] type) {
        filename = name + ".hdr";
        this.numColumns = numColumns;
        this.type = type;
        hfColumns = new Heapfile[numColumns];
        hfNames = new String[numColumns];
        cdpinfo = new ColumnDataPageInfo(filename, numColumns,type);
        int i = 0;
        try {
            columnarFile = new Heapfile(filename);
            for (AttrType attr : type) {
                if(attr.attrType==AttrType.attrString)
                    tupleLength+=globalVar.sizeOfStr;
                else
                    tupleLength+=globalVar.sizeOfInt;
                hfNames[i] = name.concat(Integer.toString(i));
                hfColumns[i] = new Heapfile(hfNames[i]);
                i++;
            }
            cdpinfo.setHfNames(hfNames);
            cdpinfo.setTupleLength(tupleLength);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void deleteColumnarFile() {
        try {
            for (Heapfile hf : hfColumns) {
                hf.deleteFile();
            }
            columnarFile.deleteFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public TID insertTuple(byte[] tuplePtr) throws SpaceNotAvailableException {
        TID tid=new TID();
        if (tuplePtr.length >= MAX_SPACE)
            throw new SpaceNotAvailableException(null, "ColumarFile:- Space not available");
        return tid;
    }
    public int getTupleCnt() throws HFBufMgrException, IOException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException {
        return hfColumns[0].getRecCnt();
    }
    /*
    public TupleScan openTupleScan(){

    }
    public Scan openColumnScan(int columnNo){

    }*/
    public boolean updateTuple(TID tid, Tuple newtuple)throws InvalidUpdateException{
        boolean status=false;
        if(tupleLength!=newtuple.getLength())
            throw new InvalidUpdateException(null,"ColumnarFile:- Invalid Tuple Update");

        return status;
    }


}

