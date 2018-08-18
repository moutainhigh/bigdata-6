import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by liuxinyuan on 2018/1/15.
 */
public class Aa {
    public static void main(String[] args){
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
          System.out.println(format.parse("2017-12-02 00:12:12").getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
