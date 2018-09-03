package myApacheStorm;

import joinery.DataFrame;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SaveIntoDB {
    public static void main(String[] args) {

    }
//            poiID:java.lang.Long
//            theme:java.lang.String
//            subTheme:java.lang.String
//            poiName:java.lang.String
//            lat:java.lang.Double
//            long:java.lang.Double
    public static boolean savePoiMelb(){
        boolean isSuccessful = false;
        String POIpath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_code_data/POI-Melb.csv";
        Connection conn = null;
        PreparedStatement stmt = null;
        try{
            Class.forName(Conf.JDBC_DRIVER);
            conn = DriverManager.getConnection(Conf.DB_URL + "?useSSL=FALSE&serverTimezone=UTC", Conf.user, Conf.password);
            DataFrame<Object> dfMelbPOI = new joinery.DataFrame<>();
            dfMelbPOI = joinery.DataFrame.readCsv(POIpath, ";");
            for (int i = 0; i < dfMelbPOI.length(); i++) {
                String str = "insert into PoiMelb values (?, ?, ?, ?, ?, ?)";
                stmt = conn.prepareStatement(str);
                stmt.setLong(1,(Long)dfMelbPOI.col("poiID").get(i));
                stmt.setString(2,dfMelbPOI.col("theme").get(i).toString());
                stmt.setString(3,dfMelbPOI.col("subTheme").get(i).toString());
                stmt.setString(4,dfMelbPOI.col("poiName").get(i).toString());
                stmt.setDouble(5,(Double)dfMelbPOI.col("lat").get(i));
                stmt.setDouble(6,(Double)dfMelbPOI.col("long").get(i));
                try{
                    stmt.executeUpdate();
                }
                catch(Exception e){
                    System.out.println("Save poi-Melb failed.");
                    e.printStackTrace();
                    return false;
                }
            }
            System.out.println("Save poi-Melb successfully.");
            isSuccessful = true;
        }catch (Exception e){
            System.out.println("Save poi-Melb failed.");
            return false;
        }
        return isSuccessful;
    }


//                poiID:
//                java.lang.Long
//                tweetID:
//                java.lang.Double
//                userID:
//                java.lang.Long
//                created_at:
//                java.lang.String
//                melbTime_created_at:
//                java.lang.String
//                createdWeekday:
//                java.lang.Long
//                createdHour:
//                java.lang.Long
//                createdDay:
//                java.lang.Long
//                createdMonth:
//                java.lang.Long
//                createdYear:
//                java.lang.Long
//                lat:
//                java.lang.Double
//                long:java.lang.Double
//                text:
//                java.lang.String
//                poiDist:
//                java.lang.Double
//                poiLat:
//                java.lang.Double
//                poiLong:
//                java.lang.Double
//                poiTheme:
//                java.lang.String
//                poiName:
//                java.lang.String
//                poiFreq:
//                java.lang.Long
    public static boolean saveTweets(){
        boolean isSuccessful = false;
        String dataPath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_code_data/userVisits-Melb-tweets.csv";
        Connection conn = null;
        PreparedStatement stmt = null;
        try{
            Class.forName(Conf.JDBC_DRIVER);
            conn = DriverManager.getConnection(Conf.DB_URL + "?useSSL=FALSE&serverTimezone=UTC", Conf.user, Conf.password);
            DataFrame<Object> dfTweets = new DataFrame<>();
            dfTweets = DataFrame.readCsv(dataPath, ",");
            for (int i = 0; i < dfTweets.length(); i++) {
                String str = "insert into userVisitsTweets values (?, ?, ?, ?, ?     , ?, ?, ?, ?, ?,     ?, ?, ?, ?, ?,    ?, ?, ?, ?)";
                stmt = conn.prepareStatement(str);
                stmt.setLong(1, (Long) dfTweets.col("poiID").get(i));
                stmt.setDouble(2, (Double) dfTweets.col("tweetID").get(i));
                stmt.setLong(3, (Long) dfTweets.col("userID").get(i));
                stmt.setString(4, dfTweets.col("created_at").get(i).toString());
                stmt.setString(5, dfTweets.col("melbTime_created_at").get(i).toString());
                stmt.setLong(6, (Long) dfTweets.col("createdWeekday").get(i));
                stmt.setLong(7, (Long) dfTweets.col("createdHour").get(i));
                stmt.setLong(8, (Long) dfTweets.col("createdDay").get(i));
                stmt.setLong(9, (Long) dfTweets.col("createdMonth").get(i));
                stmt.setLong(10, (Long) dfTweets.col("createdYear").get(i));
                stmt.setDouble(11, (Double) dfTweets.col("lat").get(i));
                stmt.setDouble(12, (Double) dfTweets.col("long").get(i));
                stmt.setString(13, dfTweets.col("text").get(i).toString());
                stmt.setDouble(14, (Double) dfTweets.col("poiDist").get(i));
                stmt.setDouble(15, (Double) dfTweets.col("poiLat").get(i));
                stmt.setDouble(16, (Double) dfTweets.col("poiLong").get(i));
                stmt.setString(17, dfTweets.col("poiTheme").get(i).toString());
                stmt.setString(18, dfTweets.col("poiName").get(i).toString());
                stmt.setLong(19, (Long) dfTweets.col("poiFreq").get(i));
                try {
                    stmt.executeUpdate();
                } catch (Exception e) {
                    System.out.println("Save tweet data failed.");
                    return false;
                }
            }
            isSuccessful= true;
            System.out.println("Save tweet data successfully.");
        }catch (Exception e){
            System.out.println("Save tweet data failed.");
            return false;
        }
        return isSuccessful;
    }
}
