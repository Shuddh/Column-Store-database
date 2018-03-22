package columnar;

import btree.IntegerKey;
import global.AttrType;
import global.*;
import global.GlobalConst;
import heap.*;
import java.*;

import java.io.IOException;
import java.util.ArrayList;

public class Columnarfile implements GlobalConst {
    private Heapfile columnarFile;
    private String filename;
    private int numColumns;
    AttrType[] type;
    public Heapfile[] hfColumns;
    public String[] hfNames;
    public int tupleLength;
    private ColumnDataPageInfo cdpinfo;
    private Heapfile hfDelTIDsFile;
    private Heapfile hfDelTIDsDataFile;
    private ArrayList<RID> delTuplesRIDs=new ArrayList<RID>();
    private static int delTupleCnt;

    public void setColumnarFile(Heapfile cf){
        this.columnarFile=cf;
    }

    public Heapfile getColumnarFile() {
        return columnarFile;
    }

    public void setFilename(String name){
        this.filename=name;
        this.cdpinfo.setColumnarFileName(name);
    }

    public String getFilename() {
        return filename;
    }

    public int getNumColumns() {
        return numColumns;
    }

    public String[] getHfNames() {
        return hfNames;
    }

    public Heapfile[] getHfColumns() {
        return hfColumns;
    }

    public int getTupleLength() {
        return tupleLength;
    }

    public ColumnDataPageInfo getCdpinfo() {
        return cdpinfo;
    }

    public AttrType[] getType() {
        return type;
    }

    public Columnarfile(String name, int numColumns, AttrType[] type) {
        this.filename = name;
        this.numColumns = numColumns;
        this.type = type;
        hfColumns = new Heapfile[numColumns];
        hfNames = new String[numColumns];
        cdpinfo = new ColumnDataPageInfo(filename, numColumns,type);
        int i = 0;
        try {
            columnarFile = new Heapfile(filename+".hdr");
            hfDelTIDsDataFile=new Heapfile(filename+".delData");
            hfDelTIDsFile=new Heapfile(filename+".delTIDs");
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

    public TID insertTuple(byte[] tuplePtr) throws SpaceNotAvailableException,
            InvalidTupleSizeException,HFException,HFBufMgrException,
            HFDiskMgrException, IOException,InvalidSlotNumberException
    {
        if (tuplePtr.length >= MAX_SPACE)
            throw new SpaceNotAvailableException(null, "ColumarFile:- Space not available");
        TID tid=new TID();
        tid.numRIDs=numColumns;
        tid.recordIDs=new RID[numColumns];
        int offset=0;
        for(int i=0;i<numColumns;i++){
            if(type[i].attrType==AttrType.attrString){
                byte[] value=new byte[globalVar.sizeOfStr];
                String content=Convert.getStrValue(offset,tuplePtr,globalVar.sizeOfStr);
                Convert.setStrValue(content,0,value);
                tid.recordIDs[i]=hfColumns[i].insertRecord(value);
                offset+=globalVar.sizeOfStr;
            }
            if(type[i].attrType==AttrType.attrInteger){
                byte[] value=new byte[globalVar.sizeOfInt];
                int content=Convert.getIntValue(offset,tuplePtr);
                Convert.setIntValue(content,0,value);
                tid.recordIDs[i]=hfColumns[i].insertRecord(value);
                offset+=globalVar.sizeOfInt;
            }
            if(type[i].attrType==AttrType.attrReal){
                byte[] value=new byte[globalVar.sizeOfInt];
                float content=Convert.getFloValue(offset,tuplePtr);
                Convert.setFloValue(content,0,value);
                tid.recordIDs[i]=hfColumns[i].insertRecord(value);
                offset+=globalVar.sizeOfInt;
            }
            if(type[i].attrType==AttrType.attrSymbol){
                byte[] value=new byte[globalVar.sizeOfChar];
                char content=Convert.getCharValue(offset,tuplePtr);
                Convert.setCharValue(content,0,value);
                tid.recordIDs[i]=hfColumns[i].insertRecord(value);
                offset+=globalVar.sizeOfChar;
            }
        }
        tid.position=hfColumns[0].getPosition(tid.recordIDs[0]);
        return tid;
    }

    public Tuple getTuple(TID tid) throws SpaceNotAvailableException,
            InvalidTupleSizeException,HFException,HFBufMgrException,
            HFDiskMgrException, IOException,InvalidSlotNumberException
    {
        Tuple tup=new Tuple();
        byte[] tupVal=new byte[tupleLength];
        int offset=0;
        Tuple temp;
        try{
        for(int i=0;i<tid.numRIDs;i++){
            temp=hfColumns[i].getRecord(tid.recordIDs[i]);
            if(type[i].attrType==AttrType.attrString){
                Convert.setStrValue(temp.getStrFld(1),offset,tupVal);
                offset=offset+globalVar.sizeOfStr;
            }
            if(type[i].attrType==AttrType.attrSymbol){
                Convert.setCharValue(temp.getCharFld(1),offset,tupVal);
                offset=offset+globalVar.sizeOfChar;
            }
            if(type[i].attrType==AttrType.attrInteger){
                Convert.setIntValue(temp.getIntFld(1),offset,tupVal);
                offset=offset+globalVar.sizeOfInt;
            }
            if(type[i].attrType==AttrType.attrReal){
                Convert.setFloValue(temp.getFloFld(1),offset,tupVal);
                offset=offset+globalVar.sizeOfInt;
            }
        }}
        catch (Exception e){
            e.printStackTrace();
        }
        tup.tupleSet(tupVal,0,tupleLength);
        return tup;
    }

    public ValueClass getValue(TID tid,int column){
        ValueClass val=null;
        IntegerValue i=new IntegerValue(0);
        StringValue s=new StringValue(null);
        try{
            Tuple t=hfColumns[column].getRecord(tid.recordIDs[column]);
            byte[] data=t.returnTupleByteArray();
            if(type[column].attrType==AttrType.attrInteger){
                i.setValue(Convert.getIntValue(0,data));
                val=i;
            }
            if(type[column].attrType==AttrType.attrString){
                s.setValue(Convert.getStrValue(0,data,data.length));
                val=s;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return val;
    }

    public int getTupleCnt() throws HFBufMgrException, IOException,
            HFDiskMgrException, InvalidSlotNumberException,
            InvalidTupleSizeException
    {
        return hfColumns[0].getRecCnt();
    }

    public TupleScan openTupleScan() throws InvalidTupleSizeException,IOException{
        TupleScan ts=new TupleScan(this);
        return ts;
    }

    public boolean updateTuple(TID tid, Tuple newtuple) throws Exception {
        boolean status=false;
        Tuple tup=null;
        if(tupleLength!=newtuple.getLength())
            throw new InvalidUpdateException(null,"ColumnarFile:- Invalid Tuple Update");
        for(int i=0;i<tid.numRIDs;i++){
            if(type[i].attrType==AttrType.attrInteger)
                tup.setIntFld(1,newtuple.getIntFld(i+1));
            if(type[i].attrType==AttrType.attrString)
                tup.setStrFld(1,newtuple.getStrFld(i+1));
            if(type[i].attrType==AttrType.attrReal)
                tup.setFloFld(1,newtuple.getFloFld(i+1));
            status=hfColumns[i].updateRecord(tid.recordIDs[i],tup);
        }
        return status;
    }

    public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column){
        boolean status=false;
        Tuple tup=null;
        try {
            if(type[column - 1].attrType == AttrType.attrInteger)
                tup.setIntFld(1, newtuple.getIntFld(column));
            if(type[column-1].attrType==AttrType.attrString)
                tup.setStrFld(1,newtuple.getStrFld(column));
            if(type[column-1].attrType==AttrType.attrReal)
                tup.setFloFld(1,newtuple.getFloFld(column));
            status=hfColumns[column-1].updateRecord(tid.recordIDs[column-1],tup);
        }catch (Exception e){
            e.printStackTrace();
        }
        return status;
    }

    public boolean markTupleDeleted(TID tid){
        boolean status=false;
        byte[] delData=new byte[tupleLength];
        byte[] delRIDs=new byte[numColumns*2*globalVar.sizeOfInt];
        int offset1=0;
        int offset2=0;
        Tuple tempData;
        try {
            for (int i = 0; i < numColumns; i++) {
                if (type[i].attrType == AttrType.attrString) {
                    tempData = hfColumns[i].getRecord(tid.recordIDs[i]);
                    Convert.setStrValue(tempData.getStrFld(1),offset1,delData);
                    offset1=offset1+globalVar.sizeOfStr;
                }
                if (type[i].attrType == AttrType.attrInteger) {
                    tempData = hfColumns[i].getRecord(tid.recordIDs[i]);
                    Convert.setIntValue(tempData.getIntFld(1),offset1,delData);
                    offset1=offset1+globalVar.sizeOfInt;
                }
                if (type[i].attrType == AttrType.attrReal) {
                    tempData = hfColumns[i].getRecord(tid.recordIDs[i]);
                    Convert.setFloValue(tempData.getFloFld(1),offset1,delData);
                    offset1=offset1+globalVar.sizeOfInt;
                }
                if (type[i].attrType == AttrType.attrSymbol) {
                    tempData = hfColumns[i].getRecord(tid.recordIDs[i]);
                    Convert.setCharValue(tempData.getCharFld(1),offset1,delData);
                    offset1=offset1+globalVar.sizeOfChar;
                }
                Convert.setIntValue(tid.recordIDs[i].pageNo.pid,offset2,delRIDs);
                Convert.setIntValue(tid.recordIDs[i].slotNo,offset2+globalVar.sizeOfInt,delRIDs);
                offset2=offset2+(2*globalVar.sizeOfInt);
            }
            hfDelTIDsFile.insertRecord(delRIDs);
            RID rid=hfDelTIDsDataFile.insertRecord(delData);
            delTuplesRIDs.add(rid);
            delTupleCnt=delTupleCnt+1;
            status=true;
        }catch (Exception e){
            e.printStackTrace();
        }
        return status;
    }

    public boolean purgeAllDeletedTuples(){
        boolean status=false;
        try {
            for (int i = 0; i < hfDelTIDsDataFile.getRecCnt();i++ ){
                if(delTuplesRIDs.size()==0) break;
                status=hfDelTIDsDataFile.deleteRecord(delTuplesRIDs.get(i));
                delTupleCnt=delTupleCnt-1;
            }
            //hfDelTIDsFile.deleteFile();
        }catch (Exception e){
            e.printStackTrace();
        }
        delTuplesRIDs.clear();
        return status;
    }

}
