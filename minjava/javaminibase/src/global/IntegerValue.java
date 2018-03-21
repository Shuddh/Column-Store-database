package global;

public class IntegerValue extends ValueClass {
    private Integer value;
    public IntegerValue(int val){
        value=new Integer(val);
    }
    public IntegerValue(Integer val){
        value=new Integer(val.intValue());
    }
    public Integer getValue(){
        return new Integer(value.intValue());
    }
    public void setValue(Integer val){
        value=new Integer(val.intValue());
    }
    public String toString(){
        return value.toString();
    }
}
