import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;
import java.util.zip.GZIPOutputStream;



/**
 * Created by ac on 3/10/16.
 */
public class ArchiverTimerTask extends TimerTask {
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yy-MM-dd");
    private SimpleDateFormat timeFormat = new SimpleDateFormat("HH-mm");
    public KafkaConsumer<String,String> consumer;
    public String dir = "";
    private List<ConsumerRecord<String,String>> buffer = new ArrayList<>();
    public ArchiverTimerTask(String inputDir,KafkaConsumer<String,String> inputCon){
        super();
        dir = inputDir;
        consumer = inputCon;
        System.out.println(consumer.subscription().toString());
    }

    public void run(){
        ConsumerRecords<String, String> records;
        try{
            records = consumer.poll(1000);
        }catch(Exception e){
            e.printStackTrace();
            return;
        }
        //File Input
        Date currTime = new Date();

        System.out.println(dateFormat.format(currTime));
        File dateDir = new File(dir + dateFormat.format(currTime));
        if(!(dateDir.exists())){
            dateDir.mkdirs();
        }
        for (ConsumerRecord<String, String> record : records) {
            //System.out.println(record.value());
            buffer.add(record);
        }
        if((currTime.getTime() % (1000*60*10)) < 1000) {
            try {
                FileOutputStream file = new FileOutputStream(dir + dateFormat.format(currTime) + "/" + timeFormat.format(currTime) + ".json.gz");
                GZIPOutputStream output = new GZIPOutputStream(file);
                for (ConsumerRecord<String, String> record : buffer) {
                    output.write((record.value() + "\n").getBytes() );
                    //System.out.println(record.value());
                }
                output.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            buffer.clear();
        }
        System.out.println(timeFormat.format(currTime));

    }
}

