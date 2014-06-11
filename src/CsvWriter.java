import com.mongodb.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Date;
import java.util.Vector;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhb on 14-2-24.
 */
public abstract class CsvWriter extends Writer {
    private DBCollection coll = null;
    Vector v1 = null;
    String strHead=null;
    public CsvWriter(Vector v_colName,String strHead,DBCollection coll){
        this.v1=v_colName;
        this.strHead=strHead;
        this.coll=coll;
    }
    private void Writer() {}


    public void Writer(String sPath) {
        try {
            final FileWriter fileWriter = new FileWriter(sPath, true);
            fileWriter.write(strHead);
            final ScheduledExecutorService timerRead = Executors.newScheduledThreadPool(1);
            timerRead.scheduleAtFixedRate(
                    new Runnable() {
                        public void run() {
                            try {
                                StringBuffer strbuffer = TaskRead(v1);
                                fileWriter.write(strbuffer.toString());
                                fileWriter.flush();
                            } catch (IOException ex) {
                            }
                        }
                    },
                    0,
                    1000,
                    TimeUnit.MILLISECONDS);

            timerRead.schedule(new Runnable() {
                public void run() {
                    timerRead.shutdownNow();
                    try {
                        //if (timerRead.isShutdown()) {
                        fileWriter.close();
                        //}
                    } catch (Exception ex) {
                    }
                }
            }, 86400, TimeUnit.SECONDS);

        } catch (Exception ex) {

        }
    }

    ////定时读取数据库中数据
    private StringBuffer TaskRead(Vector v_col) {
        StringBuffer strBuffer = new StringBuffer();
        try {

            Calendar calener = Calendar.getInstance();
            Date d1 = calener.getTime();
            Date d2 = new Date(calener.getTime().getTime() - 3000);
            BasicDBObject b2 = new BasicDBObject();
            b2.put("$gte", d2);
            b2.put("$lte", d1);
            DBCursor cursor = coll.find(new BasicDBObject("_id", b2)).sort(new BasicDBObject("_id", -1)).limit(1);
            String sValue = "";
            while (cursor.hasNext()) {
                DBObject dbo = cursor.next();
                int count = v_col.size();
                for (int i = 0; i < count; i++) {
                    String name = v_col.get(i).toString();
                    String value = "";
                    if (dbo.keySet().contains(name)) {
                        value = dbo.get(name).toString();
                    } else {
                        value = "0";
                    }
                    sValue += value + ",";
                }
                sValue = sValue.substring(0, sValue.length() - 1) + "\r\n";
                strBuffer.append(sValue);
            }
            return strBuffer;

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return null;
        }
    }

}
