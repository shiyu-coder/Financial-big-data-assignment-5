package test;

public class WCString implements Comparable<WCString>{
    private int key;
    private String value;

    public WCString(Integer count, String word){
        this.key = count;
        this.value = word;
    }

    public String getValue() {
        return value;
    }
    public int getKey() {
        return key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int compareTo(WCString o) {
        if(key == o.getKey()){
            return value.compareTo(o.getValue());
        }else {
            return key - o.getKey();
        }
    }
}
