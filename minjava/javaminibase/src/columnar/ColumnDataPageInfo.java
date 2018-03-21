package columnar;

import global.AttrType;
import global.GlobalConst;
import global.*;
import heap.Tuple;

import java.io.IOException;

public class ColumnDataPageInfo implements GlobalConst {
    private String columnarFileName;
    private static int tupleLength;
    private int numColumns;
    private AttrType[] attrTypes;
    private String[] hfNames;

    private static int metaDataSize;
    private byte[] metaData;

    public ColumnDataPageInfo(){ }
    private int calculateMDSize(int noc){
        int size=0;
        size=globalVar.sizeOfStr+(2*globalVar.sizeOfInt)+(noc*globalVar.sizeOfInt)+(noc*globalVar.sizeOfStr);
        return size;
    }
    public ColumnDataPageInfo(String columnarFileName,int numColumns,AttrType[] attrTypes){
        this.columnarFileName=columnarFileName;
        tupleLength=0;
        this.numColumns=numColumns;
        this.attrTypes=attrTypes;
        this.hfNames=new String[numColumns];
        metaDataSize=calculateMDSize(numColumns);
        metaData=new byte[metaDataSize];
    }
    public Tuple getMetaData() throws IOException{
        int offset=0;
        Convert.setStrValue(this.columnarFileName,offset,metaData);
        offset+=globalVar.sizeOfStr;
        Convert.setIntValue(tupleLength,offset,metaData);
        offset+=globalVar.sizeOfInt;
        Convert.setIntValue(this.numColumns,offset,metaData);
        offset+=globalVar.sizeOfInt;
        for(int i=0;i<numColumns;i++){
            Convert.setIntValue(attrTypes[i].attrType,offset,metaData);
            Convert.setStrValue(hfNames[i],offset+globalVar.sizeOfInt,metaData);
            offset=offset+globalVar.sizeOfInt+globalVar.sizeOfStr;
        }
        Tuple MD=new Tuple(metaData,0,metaDataSize);
        return MD;
    }
    public String getColumnarFileName(){
        return columnarFileName;
    }
    public void setColumnarFileName(String name){
        columnarFileName=name;
    }
    public void setTupleLength(int length){
        tupleLength=length;
    }
    public int getTupleLength(){
        return tupleLength;
    }
    public void setNumColumns(int noc){
        this.numColumns=noc;
    }
    public int getNumColumns(){
        return numColumns;
    }
    public void setAttrTypes(AttrType[] attr){
        this.attrTypes=attr;
    }
    public AttrType[] getAttrTypes(){
        return attrTypes;
    }
    public void setHfNames(String[] hfNames){
        this.hfNames=hfNames;
    }
    public String[] getHfNames() {
        return hfNames;
    }
    public int getMetaDataSize(){
        return metaDataSize;
    }
}

