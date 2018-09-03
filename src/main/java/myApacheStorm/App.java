package myApacheStorm;

import joinery.DataFrame;

import javax.xml.crypto.Data;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        String POIpath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_code_data/POI-Melb.csv";
        String dataPath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_code_data/userVisits-Melb-tweets.csv";
        String savePath = "/Users/de-cheng/Documents/master degree/master project/poisson_twitter_java/";
        Integer timeBlock = 600;

        Connection conn = null;
        PreparedStatement stmt = null;
        String query = "";

        try {
            Class.forName(Conf.JDBC_DRIVER);
            conn = DriverManager.getConnection(Conf.DB_URL+"?useSSL=FALSE&serverTimezone=UTC",Conf.user,Conf.password);
            System.out.println("Linked to DB");
            DataFrame<Object> dfMelbPOI = new joinery.DataFrame<>();
            DataFrame<Object> dfTweets = new DataFrame<>();

            //In[1]
            System.out.println("\nIn[1]");
//            SaveIntoDB.savePoiMelb();
//            SaveIntoDB.saveTweets();
            // Save PoiMelb and tweets into DB

            //In[2]
            System.out.println("\nIn[2]");
            query = "SELECT count(distinct(tweetID)) from userVisitsTweets";
            stmt = conn.prepareStatement(query);
            try{
                ResultSet result = stmt.executeQuery();
                while(result.next()){
                    //Retrieve by column name
                    System.out.println("Number of Tweets: " + result.getInt("count(distinct(tweetID))"));
                }
                result.close();
            }
            catch(SQLException e){
                e.printStackTrace();
            }
            query = "SELECT count(distinct(userID)) from userVisitsTweets";
            stmt = conn.prepareStatement(query);
            try{
                ResultSet result = stmt.executeQuery();
                while(result.next()){
                    //Retrieve by column name
                    System.out.println("Number of Users: " + result.getInt("count(distinct(userID))"));
                }
                result.close();
                }
            catch(SQLException e){
                e.printStackTrace();
            }
            query = "SELECT tweetID,text from userVisitsTweets where tweetID=(select min(tweetID) from userVisitsTweets)";
            stmt = conn.prepareStatement(query);
            try{
                ResultSet result = stmt.executeQuery();
                while(result.next()){
                    //Retrieve by column name
                    System.out.println("min tweetID: "+result.getDouble("tweetID") + "\ntext: " + result.getString("text"));
                }
                result.close();
            }
            catch(SQLException e){
                e.printStackTrace();
            }
            query = "SELECT tweetID,text from userVisitsTweets where tweetID=(select max(tweetID) from userVisitsTweets)";
            stmt = conn.prepareStatement(query);
            try{
                ResultSet result = stmt.executeQuery();
                while(result.next()){
                    //Retrieve by column name
                    System.out.println("max tweetID: "+result.getDouble("tweetID") + "\ntext: " + result.getString("text"));
                }
                result.close();
            }
            catch(SQLException e){
                e.printStackTrace();
            }

            //In[3]
            System.out.println("\nIn[3]");
            query = "SELECT tweetID,poiID,melbTime_created_at from userVisitsTweets where createdYear=2017&&createdMonth=1";
            stmt = conn.prepareStatement(query);
            Double tweetID = null;
            String melbTime_created_at = null;
            String unixtime = "";
            String unixtimeMin = "";
            Long poiID = null;

            dfTweets = new DataFrame<>("tweetID","poiID","melbTime_created_at","unixtimeMin");
            try{
                ResultSet result = stmt.executeQuery();
                while(result.next()){
                    //Retrieve by column name
                    tweetID = result.getDouble("tweetID");
                    poiID = result.getLong("poiID");
                    melbTime_created_at = result.getString("melbTime_created_at");
                    unixtimeMin = String.valueOf(unixtimeMin(melbTime_created_at));
                    dfTweets.append(Arrays.asList(tweetID,poiID,melbTime_created_at,unixtimeMin));
                }
                result.close();
            }
            catch(SQLException e){
                e.printStackTrace();
            }

            //In[4]
//  CREATE TABLE `testDB0`.`201701tweet` (
//  `tweetID` DOUBLE NOT NULL,
//  `melbTime_created_at` VARCHAR(100) NOT NULL,
//  `unixtime` VARCHAR(100) NULL,
//  `unixtimeMin` VARCHAR(100) NULL,
//  PRIMARY KEY (`tweetID`));
            System.out.println("\nIn[4]");
            boolean isSuccessful = true;
//            isSuccessful = saveTestTweets(conn,stmt,query,dfTweets);
            if(isSuccessful){
                System.out.println("Save tweets of 2017-01 successfully!");
            }
            else {
                System.out.println("Failed to save tweets of 2017-01.");
            }

            //In[5]
            System.out.println("\nIn[5]");

            //In[6]
            System.out.println("\nIn[6]");
            Long pastWinSize = Long.valueOf(3 * 24 * 60 * 60);
            query = "SELECT min(unixtimeMin) from 201701tweet";
            Long minTime = null;
            Long maxTime = null;
            try{
                stmt = conn.prepareStatement(query);
                ResultSet result = stmt.executeQuery();
                while(result.next()){
                    //Retrieve by column name
                    unixtimeMin = result.getString("min(unixtimeMin)");
                }
                minTime = Long.parseLong(unixtimeMin) + pastWinSize;
            }catch (SQLException e){
                e.printStackTrace();
            }
            query = "SELECT max(unixtimeMin) from 201701tweet";
            try{
                stmt = conn.prepareStatement(query);
                ResultSet result = stmt.executeQuery();
                while(result.next()){
                    //Retrieve by column name
                    unixtimeMin = result.getString("max(unixtimeMin)");
                }
                maxTime = Long.parseLong(unixtimeMin);
            }catch (SQLException e){
                e.printStackTrace();
            }

            System.out.println("minTime: "+minTime);//1483448400
            System.out.println("maxTime: "+maxTime);//1485865800
//
//SELECT poiID,count(*) as totalTweets,min(unixtimeMin) as firstTweetTime FROM testDB0.201701tweet group by poiID;
            query = "SELECT poiID,count(*) as totalTweets,min(unixtimeMin) as firstTweetTime FROM 201701tweet where unixtimeMin<"+ minTime +" group by poiID";
            Long count = null;
            String firstTweetTime = null;
            DataFrame<Object> dfLambdas = new DataFrame<Object>("poiID","totalTweets","firstTweetTime");
            try{
                stmt = conn.prepareStatement(query);
                ResultSet result = stmt.executeQuery();
                while(result.next()){
                    //Retrieve by column name
                    poiID = result.getLong("poiID");
                    count = result.getLong("totalTweets");
                    firstTweetTime = result.getString("firstTweetTime");
                    dfLambdas.append(Arrays.asList(poiID,count,firstTweetTime));
                }
//                dfLambdas.show();
            }catch (SQLException e){
                e.printStackTrace();
            }

            //In[7]
            System.out.println("\nIn[7]");
            DataFrame<Object> dfResults = new DataFrame<Object>("algo", "poiID", "timePeriod","eventSignal", "eventDetected");
            DataFrame<Object> dfDetectedEvents = new DataFrame<Object>("algo", "poiID", "eventSignal","timeStarted","prevTime","timeEnded");
            Long start_time1= System.currentTimeMillis();
            Long eventID = Long.valueOf(0);
            //******************* Detection start from here









        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static long unixtimeMin(String melbTime_created_at){
        DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd hh:mm:ss +1100 yyyy",Locale.ENGLISH);
        try{
            Date date = dateFormat.parse(melbTime_created_at);
            long unixTime = (long) date.getTime()/1000;
            long unixTimeMin = unixTime-unixTime%600;
            return unixTimeMin;
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return 0;
    }

    public static boolean saveTestTweets(Connection conn,PreparedStatement stmt,String query,DataFrame<Object> dfTweets){
        boolean isSuccessful = false;
        query = "insert into 201701tweet values (?, ?, ?, ?, ?)";
        try {
            stmt = conn.prepareStatement(query);
            for(int i = 0;i<dfTweets.length();i++){
                Double tweetID = (Double)dfTweets.col("tweetID").get(i);
                String melbTime_created_at = dfTweets.col("melbTime_created_at").get(i).toString();
                String unixtimeMin = dfTweets.col("unixtimeMin").get(i).toString();
                Long poiID = (Long)dfTweets.col("poiID").get(i);
                stmt.setLong(5,poiID);
                stmt.setDouble(1,tweetID);
                stmt.setString(2,melbTime_created_at);
                stmt.setString(3,"");
                stmt.setString(4,unixtimeMin);
                stmt.executeUpdate();
            }
            isSuccessful=true;

        }catch (SQLException e){
            e.printStackTrace();
            return false;
        }

        return isSuccessful;
    }
}
