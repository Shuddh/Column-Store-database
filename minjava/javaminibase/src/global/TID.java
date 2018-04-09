//New tuple ID class TID. should reside in following path global/TID
package global;
import java.io.*;
import java.util.ArrayList;


public class TID{
	public int numRIDs;
	public int position;
	public RID[] recordIDs;

	public TID(){}
	public TID(int numRIDs){
		this.numRIDs = numRIDs;
	}
	public TID(int numRIDs, int position){
		this.numRIDs=numRIDs;
		this.position=position;
	}
	public TID(int numRIDs, int position, RID[] recordIDs){
		this.numRIDs=numRIDs;
		this.position=position;
		this.recordIDs=recordIDs;
	}
	public void copyTid(TID tid){
		numRIDs=tid.numRIDs;
		position=tid.position;
		recordIDs=tid.recordIDs;
	}
	private boolean compare(RID[] rcrdids){
		if(recordIDs.length!=rcrdids.length)
			return false;
		int i=0;
		for(RID rid: recordIDs){
			if(rid.slotNo==rcrdids[i].slotNo && rid.pageNo.pid==rcrdids[i].pageNo.pid ) {
				i = i + 1;
				continue;
			}
			else
				return false;
		}
		return true;
	}

	public boolean equals(TID tid){
		if(tid==null)
			return false;
		if(numRIDs==tid.numRIDs && position==tid.position && compare(tid.recordIDs))
			return true;
		else
			return false;
	}
	public void writeToByteArray(byte[] array, int offset) throws java.io.IOException{
		Convert.setIntValue(numRIDs,offset,array);
		Convert.setIntValue(position,offset+4,array);
		for(RID rid: recordIDs){
			offset=offset+8;
			rid.writeToByteArray(array,offset);
		}
	}
	public void setPosition(int position){
		this.position=position;
	}
	public void setRID(int column,RID recordID){
		this.recordIDs[column-1]=recordID;
	}
	

}


