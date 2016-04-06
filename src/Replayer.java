import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by ac on 3/25/16.
 */
public class Replayer {

    private KafkaProducer<String,String> producer;
    private String brokers;
    private Date startTime;
    private String dataDir;
    private SimpleDateFormat sdf;

    public Replayer(String givenBrokers, String givenTargetTopic,Date givenStartTime, String givenDirectory){

        this.brokers = givenBrokers;
        this.startTime = givenStartTime;
        this.dataDir = givenDirectory;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<String, String>(props);
        this.sdf = new SimpleDateFormat("yy-MM-dd/HH-mm");
    }
    public void start(){
        int i = 1;
        long timeDiff = System.currentTimeMillis() -  (startTime.getTime());

        while (true) {
            String dir = dataDir + sdf.format(new Date(startTime.getTime() + 10 * 60 * 1000 * i)) + ".json.gz";
            System.out.println(dir);
            ReplayerSingleFileTask rsft = new ReplayerSingleFileTask(timeDiff, dir, producer);
            rsft.run();
            i++;
        }
    }



    private class ReplayerSingleFileTask implements Runnable{
        public ReplayerSingleFileTask(long givenTimeDiff, String givenFileDir,KafkaProducer<String,String> givenProducer){
            fileDir = givenFileDir;
            producer = givenProducer;
            timeDiff = givenTimeDiff;
        }
        private String fileDir;
        private long timeDiff;
        private KafkaProducer<String,String> producer;
        public void run(){
            try{
                BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream( new FileInputStream(fileDir))));
                String raw;
                while((raw = in.readLine())!=null){
                    String timestamp = "";
                    if(raw.charAt(raw.length()-2) == '}'){ //Part of a delete
                        timestamp = raw.substring(raw.length()-16,raw.length()-3);
                    }else{
                        timestamp = raw.substring(raw.length()-15,raw.length()-2);
                    }
                    long delayTime = Long.decode(timestamp) + timeDiff;
                    while(System.currentTimeMillis() < delayTime){}
                    System.out.println(System.currentTimeMillis() - delayTime);
                    ProducerRecord<String,String> msg = new ProducerRecord<String,String>("Tweet", null,raw);
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

        Replayer replayer = new Replayer(brokers,topic,startDate,dataDir);
        replayer.start();
    }
}
