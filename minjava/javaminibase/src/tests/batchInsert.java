

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;
import global.AttrType;
import global.Convert;
public class batchInsert {
	
public static void main (String argv[]){	
		String[] colnames = {};
		AttrType[] type = {};
		int stringsize = 0;
		String filepath = "C:/Users/varun ch/Desktop/HASITHA/academix/ASU/DBMSI/minjava/javaminibase/src/tests/";
		try {
		FileInputStream fin = new FileInputStream(filepath+argv[0]);
		DataInputStream din = new DataInputStream(fin);
		BufferedReader bin = new BufferedReader(new InputStreamReader(din));

		/* code that reads first line, reads schema and creates Columnarfile */
		String line = bin.readLine();
		String[] stringArray = line.split("\\s+");
		int  tuplelength = 0;
		String[] columns = new String[stringArray.length];
		
		for(int i=0;i<stringArray.length;i++)
		{
			
			String[] temp = stringArray[i].split(":");
			String[] temp2 = temp[1].split("\\(|\\)");
			colnames[i] = temp2[0];
			if (temp2[0].equals("int"))
			{
				type[i] = new AttrType(AttrType.attrInteger);
				tuplelength = tuplelength + 4;
			}
			else {
				type[i] = new AttrType(AttrType.attrString);
				stringsize = Integer.parseInt(temp2[1]);
				tuplelength = tuplelength + Integer.parseInt(temp2[1]);
			}
			
		}
		Columnarfile cf = new Columnarfile (argv[2],Integer.parseInt(argv[3]),type);
		cf.setColumnNames(colnames);
		cf.setColumnarFileInfo(stringsize);
		System.out.println("start inserting tuples ..");
		/* start parsing and inserting records */
		
		byte [] tupledata = new byte[tuplelength];
		int offset = 0, rec =1;
		while((line = bin.readLine()) != null)
		{
			String[] temp= line.split("\\s+");
			int k=0;
			for(AttrType attr: type)
			{
				String value = temp[k];
				if(attr.attrType == AttrType.attrInteger)
				{
					Convert.setIntValue(Integer.parseInt(value), offset, tupledata);
					offset = offset + 4;
				}
				else if (attr.attrType == AttrType.attrString)
				{
					//System.out.println("offset: "+offset);
					Convert.setStrValue(value, offset, tupledata);
					offset = offset + stringsize;
				}
				k++;
			}
			cf.insertTuple(tupledata);
			offset = 0;
		
			Arrays.fill(tupledata, (byte)0);
			}
		} catch (FileNotFoundException e) {
	e.printStackTrace();
} catch (IOException e) {
	e.printStackTrace();
}
	}
}
		