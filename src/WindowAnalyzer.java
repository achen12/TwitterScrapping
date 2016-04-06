import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Timer;

/**
 * Created by ac on 3/10/16.
 */

public class WindowAnalyzer {

    public final KafkaConsumer<String,String> consumer;
    public WindowAnalyzer(String brokers){
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String,String>(props);
        consumer.subscribe(Arrays.asList("Tweet"));
    }
    public static void main(String[] args){

        String propFile = "config/analyzer.config";
        if(args.length >0){
            propFile = args[0];
        }

        String brokers = "localhost:9092";
        Properties configProp = new Properties();
        try {
            FileInputStream in = new FileInputStream(propFile);
            configProp.load(in);
            in.close();
            brokers = configProp.getProperty("Brokers");
            System.out.println("Readout success");
        }catch(Exception e){
            System.out.println("Configuration readout error; most likely config file not found.");
            e.printStackTrace();
        }


        WindowAnalyzer archiver = new WindowAnalyzer(brokers);
        Timer t = new Timer();
        ArchiverTimerTask task = new ArchiverTimerTask(dataDir,archiver.consumer);
        t.schedule(task,1,1000);
        System.out.println("Hello!");
    }
}
