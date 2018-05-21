package iterator;


import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;


import java.lang.*;
import java.io.*;

/**
 *open a heapfile and according to the condition expression to get
 *output file, call get_next to get all tuples
 */
public class Colscan extends  Iterator
{
    private AttrType[] _in1;
    private short in1_len;
    private short[] s_sizes;
    private Heapfile f;
    private Scan scan;
    private Tuple tuple1;
    private int t1_size;
    private int count=0;




    /**
     *constructor
     *@param file_name heapfile to be opened
     *@param in1[]  array showing what the attributes of the input fields are.
     *@param s1_sizes[]  shows the length of the string fields.
     *@param len_in1  number of attributes in the input tuple
     *@param n_out_flds  number of fields in the out tuple
     *@param proj_list  shows what input fields go where in the output tuple
     *@param outFilter  select expressions
     *@exception IOException some I/O fault
     *@exception FileScanException exception from this class
     *@exception TupleUtilsException exception from this class
     *@exception InvalidRelation invalid relation
     */
    public  Colscan (String  file_name,
                      AttrType in1[],
                      short s1_sizes[],
                      short     len_in1
    )
            throws IOException,
            FileScanException,
            TupleUtilsException,
            InvalidRelation
    {
  //      System.out.println("enter");
        _in1 = in1;
        in1_len = len_in1;
        s_sizes = s1_sizes;
        if(in1[0].attrType == AttrType.attrString) {
            t1_size=54;
        }
        else
            t1_size=8;

        tuple1 =  new Tuple();

        try {
          //  tuple1.setHdr(in1_len, _in1, s1_sizes);
            tuple1.new_setHdr(in1_len, _in1);
        }catch (Exception e){
            throw new FileScanException(e, "setHdr() failed");
        }

        try {
            f = new Heapfile(file_name);
     //       System.out.println("filename");
     //       System.out.println(file_name);


        }
        catch(Exception e) {
            throw new FileScanException(e, "Create new heapfile failed");
        }

        try {
            scan = f.openScan();
        }
        catch(Exception e){
            throw new FileScanException(e, "openScan() failed");
        }
    }

    /**
     *@return shows what input fields go where in the output tuple
     */
    /**
     *@return the result tuple
     *@exception JoinsException some join exception
     *@exception IOException I/O errors
     *@exception InvalidTupleSizeException invalid tuple size
     *@exception InvalidTypeException tuple type not valid
     *@exception PageNotReadException exception from lower layer
     *@exception PredEvalException exception from PredEval class
     *@exception UnknowAttrType attribute type unknown
     *@exception FieldNumberOutOfBoundException array out of bounds
     *@exception WrongPermat exception for wrong FldSpec argument
     */
    public Tuple get_next()
            throws JoinsException,
            IOException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            PredEvalException,
            UnknowAttrType,
            FieldNumberOutOfBoundException,
            WrongPermat
    {
        RID rid = new RID();;

        while(true) {
            count=count+1;
            if((tuple1 =  scan.getNext(rid)) == null) {
                return null;
            }

            Tuple t = new Tuple(t1_size);

            AttrType[] New_in=new AttrType[2];
            New_in[0]=_in1[0];
            New_in[1]= new AttrType(AttrType.attrInteger);
            t.new_setHdr((short) 2, New_in);
            try {
                if(_in1[0].attrType == AttrType.attrString) {
                    t.setStrFld(1, Convert.getStrValue(0, tuple1.getTupleByteArray(),t1_size-4));
                }
                else
                    t.setIntFld(1, Convert.getIntValue(0, tuple1.getTupleByteArray()));

                t.setIntFld(2, count);
                byte[] data=t.returnTupleByteArray();
                t=new Tuple(data,0,t1_size);

            }
            catch (Exception e) {
                e.printStackTrace();
            }

            return  t;
        }
    }

    /**
     *implement the abstract method close() from super class Iterator
     *to finish cleaning up
     */
    public void close()
    {

        if (!closeFlag) {
            scan.closescan();
            closeFlag = true;
        }
    }

}


