/**
 * Created by ac on 3/10/16.
 */
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import scala.util.parsing.json.*;
import scala.xml.Null;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.*;

import twitter4j.*;
import twitter4j.JSONObject;
import twitter4j.auth.AccessToken;
public class Scrapper implements RawStreamListener{
    public KafkaProducer<String,String> producer;


    @Override
    public void onMessage(String rawString){
        ProducerRecord<String,String> msg = new ProducerRecord<String,String>("Tweetlive", null,rawString);
        producer.send(msg);
        //System.out.println(rawString);
    }
    @Override
    public void onException(Exception e){
        e.printStackTrace();
    }

    public static void main(String[] args){
        Properties configProp = new Properties();

        String propFile = "config/scrapper.config";
        if(args.length >0){
            propFile = args[0];
        }


        String consumer,consumerSecret,access,accessSecret;
        consumer = "";
        consumerSecret = "";
        access = "";
        accessSecret = "";
        int WOEID = 1;
        String brokers = "localhost:9092";


        try {
            FileInputStream in = new FileInputStream(propFile);
            configProp.load(in);
            in.close();
            brokers = configProp.getProperty("Brokers");
            consumer = configProp.getProperty("Consumer");
            System.out.println(consumer);
            consumerSecret = configProp.getProperty("ConsumerSecret");
            access = configProp.getProperty("Access");
            accessSecret = configProp.getProperty("AccessSecret");
            WOEID = Integer.parseInt(configProp.getProperty("WOEID"));
            System.out.println("Readout success");
        }catch(Exception e){
            System.out.println("Configuration readout error; most likely config file not found.");
            e.printStackTrace();
        }


        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);

        Scrapper listener = new Scrapper();
        final TrendScanner tScanner = new TrendScanner(consumer,consumerSecret,new AccessToken(access,accessSecret),WOEID);


        final KafkaProducer<String,String> trendProducer = new KafkaProducer<String, String>(props);
        TimerTask trendTask = new TimerTask(){
            public void run(){
                JSONObject obj = new JSONObject();
                List<Map<String,String>> trendList = new ArrayList<Map<String,String>>();
                for(Trend t : tScanner.scan()){
                    Map<String,String> trendObj = new HashMap<String,String>();
                    trendObj.put("Name", t.getName());
                    trendObj.put("URL", t.getURL());
                    trendList.add(trendObj);
                }
                try {
                    obj.put("Trend", trendList);
                    obj.put("timestamp_ms", System.currentTimeMillis());
                }catch(Exception e){
                    e.printStackTrace();
                }
                ProducerRecord<String,String> msg = new ProducerRecord<String,String>("Trendlive", null,obj.toString());
                System.out.println(obj.toString());
                trendProducer.send(msg);
            }
        };

        listener.producer = producer;

        TwitterStream stream = new TwitterStreamFactory().getInstance();
        stream.setOAuthConsumer(consumer,consumerSecret);
        stream.setOAuthAccessToken(new AccessToken(access, accessSecret ));
        stream.addListener(listener);

        Timer t = new Timer();

        t.schedule(trendTask,1000,270000);
        FilterQuery fq = new FilterQuery();
        double[][] locationq = {{-125.0011,24.9493},{-66.9326,49.5904}};
        fq.locations(locationq);
        stream.filter(fq);

    }
}
