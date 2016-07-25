/**
 * Created by ac on 7/24/16.
 */


import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;


import twitter4j.*;
import twitter4j.JSONObject;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

public class Querier {

    public static void main(String[] args){
        Properties configProp = new Properties();

        String propFile = "config/querier.config";
        if(args.length >0){
            propFile = args[0];
        }


        String consumer,consumerSecret,access,accessSecret;
        consumer = "";
        consumerSecret = "";
        access = "";
        accessSecret = "";
        String hashtag = "";
        String dateSince = "";
        String dateUntil = "";
        String brokers = "localhost:9092";
        String dir = "";

        try {
            FileInputStream in = new FileInputStream(propFile);
            configProp.load(in);
            in.close();
            consumer = configProp.getProperty("Consumer");
            System.out.println(consumer);
            consumerSecret = configProp.getProperty("ConsumerSecret");
            access = configProp.getProperty("Access");
            accessSecret = configProp.getProperty("AccessSecret");
            hashtag = "#"+configProp.getProperty("Hashtag");
            dateSince = configProp.getProperty("DateSince");
            dateUntil = configProp.getProperty("DateUntil");
            dir = configProp.getProperty("TargetDirectory");
            System.out.println("Readout success");
        }catch(Exception e){
            System.out.println("Configuration readout error; most likely config file not found.");
            e.printStackTrace();
        }


        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);
        Twitter twitter = new TwitterFactory(cb.build()).getInstance();
        twitter.setOAuthConsumer(consumer,consumerSecret);
        twitter.setOAuthAccessToken(new AccessToken(access,accessSecret));
        Integer tweetTotal = new Integer(0);

        try {
            FileOutputStream file = new FileOutputStream(dir+"/" + "Query" + hashtag + dateSince + dateUntil + ".json.gz");
            GZIPOutputStream output = new GZIPOutputStream(file);
            Query query = new Query();
            query.setQuery(hashtag);
            query.setCount(99);
            query.setResultType(Query.ResultType.recent);
            query.setSince(dateSince);
            query.setUntil(dateUntil);
            QueryResult result;
            do { //Dervied from the Twitter4J example.
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();

                tweetTotal = tweetTotal + tweets.size();
                System.out.println("Total: " + tweetTotal);
                System.out.println(result.getRateLimitStatus());

                for (Status tweet : tweets) {
                    output.write((TwitterObjectFactory.getRawJSON(tweet) + "\n").getBytes());
                }
                output.flush();
            } while ((query = result.nextQuery()) != null);
            output.close();
            System.exit(0);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        }  catch(Exception e){
            e.printStackTrace();
        }



    }
}
