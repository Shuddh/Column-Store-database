package tests;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class run {
    public static String[] runTests(){
        int choice=0;
        String inp="";
            System.out.println("[1] Batch Insert");
            System.out.println("[2] Index");
            System.out.println("[3] Query");
            System.out.println("[4] Delete");

            try{
                choice=getChoice();
                switch (choice){
                    case 1:
                        System.out.println("enter the inputs by giving space between them");
                        inp=getInput();
                        break;
                    case 2:
                    	System.out.println("enter the inputs by giving space between them");
                        inp=getInput();
                        break;
                     
                    case 3:
                    	System.out.println("enter the inputs by giving space between them");
                        inp=getInput();
                        break;

                    case 4:
                        System.out.println("enter the inputs by giving space between them");
                        inp=getInput();
                        break;
                }
            }
            catch (Exception e){
                e.printStackTrace();
            }
            String[] inp1=new String[2];
            inp1[0]=Integer.toString(choice);
            inp1[1]= inp;
        return inp1;
    }

    public static String[] main() {
      //  run i=new run();
    	System.out.println("Enter");
       String[] inp= runTests();
       System.out.println(inp);
      return inp;  
    }



    public static int getChoice () {
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        int choice = -1;
        try {
            choice = Integer.parseInt(in.readLine());
        }
        catch (NumberFormatException e) {
            return -1;
        }
        catch (IOException e) {
            return -1;
        }
        return choice;
    }
    public static String getInput() throws IOException {
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        return new String(in.readLine());

    }
}


