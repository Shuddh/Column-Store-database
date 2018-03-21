package bitmap;

import java.io.IOException;

import btree.NodeType;
import btree.PinPageException;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.DiskMgrException;
import diskmgr.FileEntryNotFoundException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.Convert;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidUpdateException;
import heap.Scan;
import heap.Tuple;

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
	private KeyClass value;

	public BitMapHeaderPage getBitMapHeaderPage() {
		return bitMapHeaderPage;
	}

	public BitMapFile(String filename) throws GetFileEntryException, PinPageException, ConstructPageException {
		headerPageId = getFileEntry(filename);
		bitMapHeaderPage = new BitMapHeaderPage(headerPageId);
		this.filename = new String(filename);
	}

	// TODO Column AR
	public BitMapFile(String filename, Columnarfile cFile, int columnNo, KeyClass value)
			throws GetFileEntryException, ConstructPageException, IOException, AddFileEntryException, HFException,
			HFBufMgrException, HFDiskMgrException {
		super(filename);
		headerPageId = getFileEntry(filename);
		bitMapHeaderPage = new BitMapHeaderPage();
		headerPageId = bitMapHeaderPage.getPageId();
		bitMapHeaderPage.set_magic0(MAGIC0);
		bitMapHeaderPage.set_rootId(new PageId(INVALID_PAGE));
		bitMapHeaderPage.set_keyType(new Character('1'));
		bitMapHeaderPage.set_maxKeySize(2);
		bitMapHeaderPage.setType(NodeType.BTHEAD);
		this.setcFile(cFile);
		this.setColumnNo(columnNo);
		this.setValue(value);
		this.filename = new String(filename);
	}

	public void setValue(KeyClass value) {
		this.value = value;
	}

	public void setColumnNo(int columnNo) {
		this.columnNo = columnNo;
	}

	public void setcFile(Columnarfile cFile) {
		this.cFile = cFile;
	}

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

	public void destroyBitMapFile() throws PageUnpinnedException, IOException, FreePageException,
			DeleteFileEntryException, IteratorException, PinPageException, ConstructPageException, UnpinPageException,
			FileEntryNotFoundException, FileIOException, InvalidPageNumberException, DiskMgrException {

		SystemDefs.JavabaseDB.delete_file_entry(filename);
		unpinPage(headerPageId);
		freePage(headerPageId);
		bitMapHeaderPage = null;
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

	boolean delete(int position) throws InvalidSlotNumberException, InvalidUpdateException, HFException,
			HFDiskMgrException, HFBufMgrException, Exception {
		boolean status = true;
		Scan scanHf = this.openScan();
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

}