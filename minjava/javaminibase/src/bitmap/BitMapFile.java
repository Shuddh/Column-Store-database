package bitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import btree.*;
import bufmgr.*;
import columnar.*;
import diskmgr.*;
import global.*;
import heap.*;

public class BitMapFile extends Heapfile {

	public static final int INVALID_PAGE = -1;
	private static final int MAGIC0 = 1989;
	private final static String lineSep = System.getProperty("line.separator");

	private BitMapHeaderPage bitMapHeaderPage;
	private PageId headerPageId;
	private String dbname;
	private String filename;
	private Columnarfile cFile;
	private int columnNo;
	public KeyClass value;



	public BitMapHeaderPage getBitMapHeaderPage() {
		return bitMapHeaderPage;
	}

	public BitMapFile(String filename) throws GetFileEntryException, PinPageException, ConstructPageException {
		//headerPageId = getFileEntry(filename);
		//bitMapHeaderPage = new BitMapHeaderPage(headerPageId);
		this.filename = new String(filename);
	}

	public BitMapFile(String name, Heapfile heapFile, KeyClass value)
			throws GetFileEntryException, ConstructPageException, IOException, AddFileEntryException, HFException,
			HFBufMgrException, HFDiskMgrException, InvalidTupleSizeException, KeyNotMatchException, InvalidSlotNumberException, SpaceNotAvailableException {
		//super(filename);
		headerPageId = getFileEntry(name);
		 if( headerPageId==null) //file not exist
		{
			 
		bitMapHeaderPage = new BitMapHeaderPage();
		headerPageId = bitMapHeaderPage.getPageId();
		bitMapHeaderPage.set_magic0(MAGIC0);
		bitMapHeaderPage.set_rootId(new PageId(INVALID_PAGE));
		bitMapHeaderPage.set_keyType(new Character('1'));
		bitMapHeaderPage.set_maxKeySize(2);
		bitMapHeaderPage.setType(NodeType.BTHEAD);
		}
		 else {
			 bitMapHeaderPage = new BitMapHeaderPage ( headerPageId );  

		 }
		//this.setValue(value);
		filename = new String(name);
		createBitMap(heapFile,value);
	}

	/*public void setColumnNo(int columnNo) {
		this.columnNo = columnNo;
	}

	public void setcFile(Columnarfile cFile) {
		this.cFile = cFile;
	} */

	private PageId getFileEntry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	public void close()
			throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
		if (bitMapHeaderPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			bitMapHeaderPage = null;
		}
	}

	public void destroyBitMapFile(String name) throws PageUnpinnedException, IOException, FreePageException,
			DeleteFileEntryException, IteratorException, PinPageException, ConstructPageException, UnpinPageException,
			FileEntryNotFoundException, FileIOException, InvalidPageNumberException, DiskMgrException {
	    System.out.println("name "+name);
		SystemDefs.JavabaseDB.delete_file_entry(name);
		
	//	unpinPage(headerPageId);
	//	freePage(headerPageId);
	//	bitMapHeaderPage = null;
	}

	private void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}

	}

	boolean delete(String name,int position) throws InvalidSlotNumberException, InvalidUpdateException, HFException,
			HFDiskMgrException, HFBufMgrException, Exception {
		boolean status = true;
		Heapfile hf=new Heapfile(name);
		Scan scanHf = hf.openScan();
		RID rid = new RID();
		Tuple tScan = new Tuple();
		int count = 0;
		while ((tScan = scanHf.getNext(rid)) != null) {
			System.out.println("Searching tuple for deletion....");
			if (count < position)
				count++;
			else
				break;
		}
		byte[] tempData = tScan.returnTupleByteArray();
		Convert.setCharValue('0', 0, tempData);
		tScan.tupleSet(tempData, tScan.getOffset(), tScan.getLength());
		status = this.updateRecord(rid, tScan);

		return status;
	}

	boolean insert(int position) throws InvalidSlotNumberException, InvalidUpdateException, HFException,
			HFDiskMgrException, HFBufMgrException, Exception {
		boolean status = true;
		Scan scanHf = this.openScan();
		RID rid = new RID();
		Tuple tScan = new Tuple();
		int count = 0;
		while ((tScan = scanHf.getNext(rid)) != null) {
			System.out.println("Searching tuple for insertion....");
			if (count < position)
				count++;
			else
				break;
		}
		byte[] tempData = tScan.returnTupleByteArray();
		Convert.setCharValue('1', 0, tempData);
		tScan.tupleSet(tempData, tScan.getOffset(), tScan.getLength());
		status = this.updateRecord(rid, tScan);
		return status;
	}
	
	 public void createBitMap(Heapfile hf,KeyClass value)
	  {
		  try
		  {
			  Scan scanHf = hf.openScan();
			 Heapfile hff =new Heapfile(filename);
			  RID rid = new RID();
			  Tuple tScan = null;
			  int cnt=0;
			  byte[] Y = new byte[4] ; byte[] N = new byte[4] ;
			Convert.setIntValue(1,0,Y);
			Convert.setIntValue(0,0,N);
			int i=0;
			  while((tScan=scanHf.getNext(rid))!=null)
			  {
				//  System.out.println(new String(tScan.getTupleByteArray()));
				  KeyClass key;
				  byte[] temp = tScan.returnTupleByteArray();
				  if (value instanceof IntegerKey) {
				//	  System.out.println(i);
					 int key1 = Convert.getIntValue(0, temp);
					 key =new IntegerKey(key1);
					 i++;
					 }
				  else {
				//	  System.out.println("str");
					  String key1= Convert.getStrValue(0, temp, globalVar.sizeOfStr).trim();
					  key= new StringKey(key1);
				  }
				  if((BT.keyCompare(value,key))==0){
					  hff.insertRecord(Y);
				  }
				  else{
					  hff.insertRecord(N);
				  }
			  }
			  scanHf.closescan();
           
		  }
		  catch(Exception e)
		  {
			 
		  }
	  }
	 
	public void printBitMapFile(String name) throws InvalidTupleSizeException, IOException, HFException, HFBufMgrException, HFDiskMgrException {
		Heapfile hf=new Heapfile(name);
		Scan scanHf = hf.openScan();
		RID rid = new RID();
		Tuple tScan = null;
		 byte[] Y = new byte[4] ; byte[] N = new byte[4] ;
			Convert.setIntValue(1,0,Y);
			Convert.setIntValue(0,0,N);
		System.out.println("Printing Bitmap contents: ");
		int cnt = 1;
		while((tScan = scanHf.getNext(rid))!=null) {
			byte[] temp = tScan.returnTupleByteArray();
			if(Arrays.equals(temp,Y)) {
				System.out.println("bitmap value at position '"+cnt+"': 1");
			} else if(Arrays.equals(temp,N)) {
				System.out.println("bitmap value at position '"+cnt+"': 0");
			} 
			cnt++;
		}
	}
		public int[] getPositions(String name) throws InvalidTupleSizeException, IOException, HFException, HFBufMgrException, HFDiskMgrException {

            Heapfile hf=new Heapfile(name);
			Scan scanHf = hf.openScan();
			RID rid = new RID();
			int[] positions= new int[hf.MAX_SPACE];
			
			Tuple tScan = null;
			 byte[] Y = new byte[4] ; 
				Convert.setIntValue(1,0,Y);
			int cnt = 1;int k=0;
			while((tScan = scanHf.getNext(rid))!=null) {
			//	System.out.println("bitmap "+new String( tScan.returnTupleByteArray()));
				byte[] temp = tScan.returnTupleByteArray();
				if(Arrays.equals(temp,Y)) {
						positions[k]=cnt;
            //        System.out.println("positions "+ positions[k]);k++;

					
				} 
				cnt++;
				
			}
			return positions;
		}
}
