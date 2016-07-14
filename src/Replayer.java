import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.JSONObject;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by ac on 3/25/16.
 */
public class Replayer extends Thread{

    private KafkaProducer<String,String> producer;
    private String brokers;
    private Date startTime;
    private String dataDir;
    private SimpleDateFormat sdf;
    private String topic;

    public Replayer(String givenBrokers, String givenTargetTopic,Date givenStartTime, String givenDirectory){

        this.brokers = givenBrokers;
        this.startTime = givenStartTime;
        this.dataDir = givenDirectory;
        this.topic = givenTargetTopic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<String, String>(props);
        this.sdf = new SimpleDateFormat("yy-MM-dd/HH-mm");
    }
    public void run(){
        int i = 1;
        long timeDiff = System.currentTimeMillis() -  (startTime.getTime());

        while (true) {
            String dir = dataDir + sdf.format(new Date(startTime.getTime() + 10 * 60 * 1000 * i)) + ".json.gz";
            System.out.println(dir);
            if ((new File(dir)).exists() == false){
                System.out.println("No next file found/ end of sequence@\n"+dir);
                break;
            }
            ReplayerSingleFileTask rsft = new ReplayerSingleFileTask(timeDiff, dir, producer,topic);
            rsft.run();
            i++;
        }
    }



    private class ReplayerSingleFileTask implements Runnable{
        public ReplayerSingleFileTask(long givenTimeDiff, String givenFileDir,KafkaProducer<String,String> givenProducer,String givenTopic){
            fileDir = givenFileDir;
            producer = givenProducer;
            timeDiff = givenTimeDiff;
            topic = givenTopic;
        }
        private String topic;
        private String fileDir;
        private long timeDiff;
        private KafkaProducer<String,String> producer;
        public void run(){
            try{
                BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream( new FileInputStream(fileDir))));
                String raw;
                while((raw = in.readLine())!=null){
                    String timestamp = "";
                    JSONObject json = new JSONObject(raw);
                    if(json.has("timestamp_ms")){ //General case like trend and tweet.
                        timestamp = json.getString("timestamp_ms");
                    }else{//Speical case:
                        //Delete object
                        timestamp = json.getJSONObject("delete").getString("timestamp_ms");
                    }
                    long delayTime = Long.decode(timestamp) + timeDiff;
                    while(System.currentTimeMillis() < delayTime){
                        //System.out.println(System.currentTimeMillis() - delayTime);
                    }
                    //System.out.println(System.currentTimeMillis() - delayTime);
                    ProducerRecord<String,String> msg = new ProducerRecord<String,String>(topic, null,raw);
                    producer.send(msg);
                }
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) {

        Properties configProp = new Properties();
        String propFile = "config/replayer.config";
        if(args.length >0){
            propFile = args[0];
        }
        String brokers,topic,dataDir;
        Date startDate;
        brokers = "localhost:9092";
        topic = "Tweet";
        startDate = new Date();
        dataDir = "";

        try {
            FileInputStream in = new FileInputStream(propFile);
            configProp.load(in);
            in.close();
            brokers = configProp.getProperty("Brokers");
            topic = configProp.getProperty("Topic");
            SimpleDateFormat sdf = new SimpleDateFormat("yy-MM-dd/HH-mm");
            startDate = sdf.parse(configProp.getProperty("StartTime"));
            dataDir = configProp.getProperty("DataDir");
            System.out.println("Readout success");
        }catch(Exception e){
            System.out.println("Configuration readout error; most likely config file not found.");
            e.printStackTrace();
        }

        Replayer replayer = new Replayer(brokers,"Tweet",startDate,dataDir);
        replayer.start();
        Replayer trendReplayer = new Replayer(brokers,"Trend",startDate,dataDir + "trend");
        trendReplayer.start();
        System.out.println("Replayers started");
    }
}
