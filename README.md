# TwitterRecordAndReplay System
A series of tools to record from Twitter and replay on Kafka.

This is an infrastructure setup for Twitter analytics.  
Scrapper taps into the Twitter Streaming and Queurying API and sends the Jsons into Kafka.
Archiver listens to the live Json on Kafka and store the data into HDFS.
Replayer picks up the data from HDFS and replay them into back into a designated Kafka topic into the analytic system.

This system allows the analytic program to be independent of the data storage and ingest.  
And this allow multiple analytics to swap back and forth between online and replay without having managing multiple access tokens.
The scalability of this set of tool into the true Twitter Stream is still in question.  Parallelizing multiple SAR sets are recommended.

More features at plan:
 * Implement RushedReplayer to replay at maximum speed.  This is mainly to test computational performance at scale rather than accurarcy performance.  Essentially rate controllable replayer.  This would be useful to test the computational performance of the analysis operations.
