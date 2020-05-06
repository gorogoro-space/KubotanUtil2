package space.gorogoro.kubotanutil2;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

class DateTimeUtil {
  public static Calendar addHour(int addHour){
      return add(null,0,0,0,addHour,0,0,0);
  }

  public static Calendar add(Calendar cal,int addYear,int addMonth,int addDay,
                                       int addHour,int addMinute,int addSecond,int addMillisecond){
      if (cal == null) {
          cal = Calendar.getInstance();
      }
      cal.add(Calendar.YEAR, addYear);
      cal.add(Calendar.MONTH, addMonth);
      cal.add(Calendar.DATE, addDay);
      cal.add(Calendar.HOUR_OF_DAY, addHour);
      cal.add(Calendar.MINUTE, addMinute);
      cal.add(Calendar.SECOND, addSecond);
      cal.add(Calendar.MILLISECOND, addMillisecond);
      return cal;
  }

  // 現在日時をyyyy/MM/dd HH:mm:ss形式で取得する.
  public static String getNowDateTime(){
    final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    final Date date = new Date(System.currentTimeMillis());
    return df.format(date);
  }

  public static long getTimestampByStrings(String str){
    try {
      DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      return df.parse(str).getTime();
    } catch (ParseException e) {
      e.printStackTrace();
      return (long) 0;
    }
  }

  public static String getStringByTimestamp(long ts){
    Date date = new Date();
    date.setTime(ts);
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
  }
}
