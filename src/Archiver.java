import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.util.*;
import java.text.SimpleDateFormat;
/**
 * Created by ac on 3/10/16.
 */

public class Archiver {

    public final KafkaConsumer<String,String> consumer;
    public Archiver(String brokers){
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String,String>(props);
        consumer.subscribe(Arrays.asList("Tweetlive"));
    }
    public static void main(String[] args){

        String propFile = "config/archiver.config";
        if(args.length >0){
            propFile = args[0];
        }

        String brokers = "localhost:9092";
        String dataDir = "/home/ac/TwitterData/";
        Properties configProp = new Properties();
        try {
            FileInputStream in = new FileInputStream(propFile);
            configProp.load(in);
            in.close();
            brokers = configProp.getProperty("Brokers");
            dataDir = configProp.getProperty("DataDir");
            System.out.println("Readout success");
        }catch(Exception e){
            System.out.println("Configuration readout error; most likely config file not found.");
            e.printStackTrace();
        }


        Archiver archiver = new Archiver(brokers);
        Timer t = new Timer();
        ArchiverTimerTask task = new ArchiverTimerTask(dataDir,archiver.consumer);
        t.schedule(task,1,1000);
        System.out.println("Hello!");
    }
}
