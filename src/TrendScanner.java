import twitter4j.Trend;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;

import java.util.TimerTask;

/**
 * Created by ac on 3/25/16.
 */

public class TrendScanner {
    private Twitter twitter;
    private int myWOEID;
    public TrendScanner(String consumer, String consumerSecret, AccessToken token, int WOEID){
        twitter = TwitterFactory.getSingleton();
        twitter.setOAuthConsumer(consumer,consumerSecret);
        twitter.setOAuthAccessToken(token);
        this.myWOEID = WOEID;
    }
    public Trend[] scan(){
        try {
            return twitter.trends().getPlaceTrends(this.myWOEID).getTrends();
        }catch(Exception e){
            e.printStackTrace();
        }
        return new Trend[0] ;
    }
}

