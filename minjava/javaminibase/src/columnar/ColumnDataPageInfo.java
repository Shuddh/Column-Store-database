package columnar;

import global.*;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;

import java.io.EOFException;
import java.io.IOException;

public class ColumnDataPageInfo implements GlobalConst {
    private String columnarFileName;
    private static int tupleLength;
    private int numColumns;
    public String[] colNames;
    private AttrType[] attrTypes;
    private int[] StringSize;
    private String[] hfNames;
    private static int metaDataSize;
    private byte[] metaData;
    private byte[] colmetadata;

    public ColumnDataPageInfo(){ }
    private int calculateMDSize(int noc){
        int size=0;
        size=(globalVar.sizeOfStr)+(2*globalVar.sizeOfInt)+(noc*globalVar.sizeOfInt)+2*(noc*globalVar.sizeOfStr);
        return size;
    }
    public ColumnDataPageInfo(String columnarFileName,int numColumns){
        this.columnarFileName=columnarFileName;
        tupleLength=0;
        this.numColumns=numColumns;
        this.attrTypes=new AttrType[numColumns];
        this.colNames=new String[numColumns];
        this.hfNames=new String[numColumns];
        metaDataSize=calculateMDSize(numColumns);
        metaData=new byte[metaDataSize];
    }
   

    public ColumnDataPageInfo ColumnDataPageInfo(String columnarFileName){
            ColumnDataPageInfo ColumnarPageInfo = new ColumnDataPageInfo();
            try	  {
                Heapfile columnarmetafile = new Heapfile(columnarFileName);
                RID rid = new RID();
                Tuple tuple = new Tuple();
                Scan hfscan = columnarmetafile.openScan();
                tuple = hfscan.getNext(rid);
                
                ColumnarPageInfo.getMetaData(tuple);
               

            }
            catch (Exception e)	  {

                e.printStackTrace();
            }

            return ColumnarPageInfo;
    }

    public Tuple setMetaData() throws IOException{
        int offset=0;
        Convert.setStrValue(this.columnarFileName,offset,metaData);
        offset+=globalVar.sizeOfStr;
        Convert.setIntValue(tupleLength,offset,metaData);
        offset+=globalVar.sizeOfInt;
        Convert.setIntValue(this.numColumns,offset,metaData);
        offset+=globalVar.sizeOfInt;
        for(int i=0;i<numColumns;i++){
            Convert.setIntValue(this.attrTypes[i].attrType,offset,metaData);
        //   Convert.setIntValue(this.StringSize[i],offset+globalVar.sizeOfInt,metaData);
            offset=offset+globalVar.sizeOfInt;
            Convert.setStrValue(this.colNames[i], offset,metaData);
            offset=offset+globalVar.sizeOfStr;
            Convert.setStrValue(this.hfNames[i], offset,metaData);

            offset=offset+(globalVar.sizeOfStr);
        }
        Tuple MD=new Tuple(metaData,0,metaDataSize);
         return MD;
    }

    public void getMetaData(Tuple metadatatuple ) throws IOException,EOFException{
        colmetadata = metadatatuple.returnTupleByteArray();
        int offset=0;
        this.columnarFileName=Convert.getStrValue(offset,colmetadata,globalVar.sizeOfStr);
        offset+=globalVar.sizeOfStr;
        this.tupleLength=Convert.getIntValue(offset,colmetadata);
         offset+=globalVar.sizeOfInt;
        this.numColumns=Convert.getIntValue(offset,colmetadata);
        offset+=globalVar.sizeOfInt;
        this.attrTypes=new AttrType[numColumns];
        this.colNames=new String[numColumns];
        this.hfNames=new String[numColumns];
        for(int i=0;i<numColumns;i++){

        	if (Convert.getIntValue(offset,colmetadata)==0)
        		this.attrTypes[i]=new AttrType(AttrType.attrString);
        	else 
        		this.attrTypes[i]=new AttrType(AttrType.attrInteger);
            offset=offset+globalVar.sizeOfInt;
    
           
            this.colNames[i]=Convert.getStrValue(offset,colmetadata,globalVar.sizeOfStr);
            offset+=globalVar.sizeOfStr;
          
            this.hfNames[i]=Convert.getStrValue(offset,colmetadata,globalVar.sizeOfStr);
        
            offset+=globalVar.sizeOfStr;
        }
    }
    public void setColumnarFileName(String name){
        this.columnarFileName=name;
    }
    public String getColumnarFileName(){
        return columnarFileName;
    }
    public void setTupleLength(int length){
        this.tupleLength=length;
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
    public void setStringSize(int[] StringSize){
        this.StringSize=StringSize;
    }
    public int[] getStringSize(){
        return StringSize ;
    }
    public AttrType[] getAttrTypes(){
        return attrTypes;
    }
    public AttrType[] getcolAttrTypes(int[] columnindexes){
    	AttrType[] attrtype = new AttrType[columnindexes.length];
    	for (int i=0;i<columnindexes.length;i++) 
    		attrtype[i]=this.attrTypes[columnindexes[i]];
    	return attrtype;
    }
    public void setColNames(String[] colnames){
        this.colNames=colnames;
    }
    public String[] getColNames(){
        return colNames;
    }

    public int[] getColindexes(String[] colnames){
        int[] colIndexes=new int[colnames.length];
      
        for(int i=0;i<colnames.length;i++){
            for(int j=0;j<this.colNames.length;j++) {
                if(colnames[i].equals(this.colNames[j])){
                    colIndexes[i]=j;
                    j=this.colNames.length;
                }
            }
        }
        return colIndexes;

    }
    public void setHfNames(String[] hfNames){
        this.hfNames=hfNames;
    }
    public String[] getHfNames() {
        return hfNames;
    }
    public String getHffile(int colindex){
    	
        return hfNames[colindex];

    }
}