import com.mongodb.*;

import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhb on 14-2-10.
 */
public class Generator {
    static Vector v_colName = new Vector();
    static String header = "_id,";
    static DBCollection coll = null;
    static Map mapKvalue=new HashMap();

    public static void main(String[] args) {
        String strName = "scada";
        try {
            InetAddress addr = InetAddress.getLocalHost();
            String strIp = addr.getHostAddress().toString();
            System.out.println("Start ..");
            Start(strIp, strName);
        } catch (Exception ex) {
        }
    }

    ///开始执行
    private static void Start(String strIp, String strName) {
        Mongo m = null;
        DB db = null;
        DBCollection meta = null;
        try {
            m = new Mongo(strIp, 27017);
            db = m.getDB(strName);
        } catch (UnknownHostException ex) {
            ex.printStackTrace();
        } catch (MongoException e) {
            e.printStackTrace();
        }
        coll = db.getCollection("DATAOUT");
        meta = db.getCollection("META");
        DBCursor cursor = meta.find();
        v_colName.add("_id");
        while (cursor.hasNext()) {
            DBObject dbo = cursor.next();
            String name = dbo.get("_id").toString();
            v_colName.add(name);
            header += name + ",";
        }
        header = header.substring(0, header.length() - 1);
        header += "\r\n";
        System.out.print(header);

        /////获取字段名及k值
        DBCursor cursor1 = meta.find();
        while (cursor1.hasNext()) {
            DBObject dbo = cursor1.next();
            double kValue = Double.parseDouble(dbo.get("kvalue").toString());
            if(kValue>0){
                String sValue = dbo.get("_id").toString();
                mapKvalue.put(sValue,kValue);
            }
        }
        cursor1.close();

        GeneratorNow();
        ///开始定时执行
        showTimer();

    }

    ////开始循环
    private static void showTimer() {
        TimerTask task = new TimerTask() {
            public void run() {
                Generator();
            }
        };
        //设置执行时间
        Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DAY_OF_MONTH);//每天
        //定制每天的24:00:00执行，
        calendar.set(year, month, day, 24, 00, 00);
        Date date = calendar.getTime();
        Timer timer = new Timer();
        System.out.println(date);
        System.out.println("每天24:00:00执行");

        int period = 86400 * 1000;
        //每天的date时刻执行task，每隔一天重复执行
        timer.schedule(task, date, period);
        //每天的date时刻执行task, 仅执行一次
        ///timer.schedule(task, date);


    }


    ////每天生成
    private static void Generator() {
        try {
            //设置执行时间
            Calendar calendar = Calendar.getInstance();
            int year = calendar.get(Calendar.YEAR);
            int month = calendar.get(Calendar.MONTH) + 1;
            int day = calendar.get(Calendar.DAY_OF_MONTH);//每天
            String csvName = "" + year + "" + month + "" + day;
            final FileWriter fw = new FileWriter("D:/csv/" + csvName + ".csv");
            fw.write(header);
            final ScheduledExecutorService timerRead = Executors.newScheduledThreadPool(1);
            timerRead.scheduleAtFixedRate(
                    new Runnable() {
                        public void run() {
                            try {
                                StringBuffer strbuffer = TaskRead(v_colName);
                                fw.write(strbuffer.toString());
                                fw.flush();
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
                        fw.close();
                        //}
                    } catch (Exception ex) {
                    }
                }
            }, 86400, TimeUnit.SECONDS);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    ///从系统开始时间生成
    private static void GeneratorNow() {
        try {
            //设置执行时间
            Calendar calendar = Calendar.getInstance();
            int year = calendar.get(Calendar.YEAR);
            int month = calendar.get(Calendar.MONTH) + 1;
            int day = calendar.get(Calendar.DAY_OF_MONTH);//每天
            long diff = 0;
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                String sDate = "" + year + "-" + month + "-" + day + " 23:59:59";
                Date d1 = df.parse(sDate);
                Date d2 = calendar.getTime();
                diff = (d1.getTime() - d2.getTime()) / 1000;
            } catch (Exception e) {
            }
            // int seconds=calendar.
            String csvName = "" + year + "" + month + "" + day;
            final FileWriter fw = new FileWriter("D:/csv/" + csvName + ".csv");
            fw.write(header);
            final ScheduledExecutorService timerRead = Executors.newScheduledThreadPool(1);
            timerRead.scheduleAtFixedRate(
                    new Runnable() {
                        public void run() {
                            try {
                                StringBuffer strbuffer = TaskRead(v_colName);
                                fw.write(strbuffer.toString());
                                fw.flush();
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
                        fw.close();
                    } catch (IOException ex) {
                    }
                }
            }, diff, TimeUnit.SECONDS);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    ////定时读取数据库中数据
    private static StringBuffer TaskRead(Vector v_col) {
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
                        if(mapKvalue.containsKey(name)){
                            double dvalue=Double.parseDouble(value);
                            double valuek=Double.parseDouble(dbo.get("kvalue").toString());
                            value=String.valueOf(valuek*dvalue);
                        }
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
