package bitmap;

import java.io.IOException;

import btree.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import heap.*;

public class BM implements GlobalConst {

	public BM() {}

	public static void printBitMap(BitMapHeaderPage header)
			throws IOException, ConstructPageException, IteratorException, HashEntryNotFoundException,
			InvalidFrameNumberException, PageUnpinnedException, ReplacerException {

		if (header.get_rootId().pid == INVALID_PAGE) {
			System.out.println("Bit Map is empty!!!");
			return;
		}
		
		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("---------------The Bit Map Structure---------------");

		System.out.println(1 + "     " + header.get_rootId());

		printBM(header.get_rootId(), "     ", 1, header.get_keyType());

		System.out.println("--------------- End ---------------");
		System.out.println("");
		System.out.println("");
	}

	// TODO Work to do
	private void printBM(PageId currentPageId, String prefix, int i, int keyType)
			{

		prefix = prefix + "       ";
		i++;

		Scan scanHf = this.openScan();
		RID rid=new RID();
		Tuple tScan=new Tuple();
		byte[] yes=new byte[2];
		byte[] no=new byte[2];
		Convert.setCharValue('0', 0, no);
		Convert.setCharValue('1', 0, yes);
		System.out.println("Printing Bitmap contents: ");
		int cnt=1;
		while((tScan=scanHf.getNext(rid))!=null) {
			byte[] temp=tScan.getData();
			if(Arrays.equals(temp,yes)) {
				System.out.println("bitmap value at position '"+cnt+"': 1");
			}
			else if(Arrays.equals(temp,no)) {
				System.out.println("bitmap value at position '"+cnt+"': 0");
			}
			cnt++;
		}
		SystemDefs.JavabaseBM.unpinPage(currentPageId, true/* dirty */);
	}
}
