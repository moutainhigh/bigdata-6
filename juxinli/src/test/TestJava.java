import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by liuxinyuan on 2018/3/6.
 */
public class TestJava {
    public static void main(String[] args){
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        System.out.print(format.format(new Date()).substring(0,10));
    }
}
