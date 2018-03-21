package global;

public class StringValue extends ValueClass {
    private String value;
    public StringValue(String s){
        value=new String(s);
    }
    public String getValue(){
        return new String(value);
    }
    public void setValue(String s){
        value=new String(s);
    }
    public String toString(){
        return value;
    }
}



