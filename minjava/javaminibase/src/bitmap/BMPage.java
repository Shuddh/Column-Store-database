package bitmap;
import java.io.*;
import java.lang.*;

import global.*;
import heap.HFPage;
import diskmgr.*;
public class BMPage extends Page implements GlobalConst {
	
	  public static final int SIZE_OF_SLOT = 4;
	  public static final int DPFIXED =  4 * 2  + 3 * 4;
	  
	  public static final int SLOT_CNT = 0;
	  public static final int USED_PTR = 2;
	  public static final int FREE_SPACE = 4;
	  //public static final int TYPE = 6;
	  public static final int PREV_PAGE = 8;
	  public static final int NEXT_PAGE = 12;
	  public static final int CUR_PAGE = 16;
	  
	  protected    short     slotCnt;   //number of slots in use
	   	  	  
	  protected    short     usedPtr;   //offset of first used byte by data records in data[]
	 	 	  
	  protected    short     freeSpace;  //number of bytes free in data[]
	    	  
	  private    PageId   prevPage = new PageId(); 	  
	  private   PageId    nextPage = new PageId(); 
	  protected    PageId    curPage = new PageId();  
	  
	  
	public BMPage(){
		//Default constructor
	}
	public BMPage(Page page)
{
		
		data =  page.getpage();
		
}
	
	public void openBMPage(Page apage) {
		
		data= apage.getpage();
	}
	
	public void init(PageId pageNo,Page apage) throws IOException
	{data = apage.getpage();
    
    slotCnt = 0;                // no slots in use
    Convert.setShortValue (slotCnt, SLOT_CNT, data);
    
    curPage.pid = pageNo.pid;
    Convert.setIntValue (curPage.pid, CUR_PAGE, data);
    
    nextPage.pid = prevPage.pid = INVALID_PAGE;
    Convert.setIntValue (prevPage.pid, PREV_PAGE, data);
    Convert.setIntValue (nextPage.pid, NEXT_PAGE, data);
    
    usedPtr = (short) MAX_SPACE;  // offset in data array (grow backwards)
    Convert.setShortValue (usedPtr, USED_PTR, data);
    
    freeSpace = (short) (MAX_SPACE - DPFIXED);    // amount of space available
    Convert.setShortValue (freeSpace, FREE_SPACE, data);
    
		
	}
	public int available_space() throws IOException {
		freeSpace = Convert.getShortValue (FREE_SPACE, data);
	     return (freeSpace - SIZE_OF_SLOT);
	}
	public boolean empty() throws IOException {
		slotCnt=Convert.getShortValue (SLOT_CNT, data);
		if(slotCnt == 0)
			return true;
		else
			return false;
	}
	public void dumpPage()throws IOException {

	      int i, n ;
	      int length, offset;
	      
	      curPage.pid =  Convert.getIntValue (CUR_PAGE, data);
	      nextPage.pid =  Convert.getIntValue (NEXT_PAGE, data);
	      usedPtr =  Convert.getShortValue (USED_PTR, data);
	      freeSpace =  Convert.getShortValue (FREE_SPACE, data);
	      slotCnt =  Convert.getShortValue (SLOT_CNT, data);
	      
	      System.out.println("DumpPage\n");
	      System.out.println("curPage= " + curPage.pid);
	      System.out.println("nextPage= " + nextPage.pid);
	      System.out.println("usedPtr= " + usedPtr);
	      System.out.println("freeSpace= " + freeSpace);
	      System.out.println("slotCnt= " + slotCnt);
	      //print (length,offset)pairs of slots
	      for (i= 0, n=DPFIXED; i < slotCnt; n +=SIZE_OF_SLOT, i++) {
	        length =  Convert.getShortValue (n, data);
		offset =  Convert.getShortValue (n+2, data);
		System.out.println("slotNo " + i +" offset= " + offset);
	        System.out.println("slotNo " + i +" length= " + length);
	      }
	}
	public byte[] getBMPage() {
		return data;
	}
	public void writeBMPageArray(byte[] array) {
		this.data=array;
		
	}
	public PageId getPrevPage()   
		    throws IOException 
		    {
		      prevPage.pid =  Convert.getIntValue (PREV_PAGE, data);
		      return prevPage;
		    }
		  
		  
		  public void setPrevPage(PageId pageNo)
		    throws IOException
		    {
		      prevPage.pid = pageNo.pid;
		      Convert.setIntValue (prevPage.pid, PREV_PAGE, data);
		    }
		  
		  
		  public PageId getNextPage()
		    throws IOException
		    {
		      nextPage.pid =  Convert.getIntValue (NEXT_PAGE, data);    
		      return nextPage;
		    }
		  
		  
		  public void setNextPage(PageId pageNo)   
		    throws IOException
		    {
		      nextPage.pid = pageNo.pid;
		      Convert.setIntValue (nextPage.pid, NEXT_PAGE, data);
		    }
		  
		  public PageId getCurPage() 
		    throws IOException
		    {
		      curPage.pid =  Convert.getIntValue (CUR_PAGE, data);
		      return curPage;
		    }
		  
		  
		  public void setCurPage(PageId pageNo)   
		    throws IOException
		    {
		      curPage.pid = pageNo.pid;
		      Convert.setIntValue (curPage.pid, CUR_PAGE, data);
		    }
		  
	}
	
	
	
