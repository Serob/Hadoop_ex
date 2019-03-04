public class StringTest {
    public static void main(String[] args) {
        String s = "asdf";
        for(String el: s.split("\\.")){
            System.out.println("EL: " + el);
        }
    }
}
