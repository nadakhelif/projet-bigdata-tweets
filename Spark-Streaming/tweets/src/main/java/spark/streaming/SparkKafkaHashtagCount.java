package spark.streaming;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkKafkaHashtagCount {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: SparkKafkaHashtagCount <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topics = args[1];
        String groupId = args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkKafkaHashtagCount")
                .getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topics)
                .option("kafka.group.id", groupId)
                .load();

        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .as(Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
            .flatMap((FlatMapFunction<Tuple2<String, String>, String>) value -> Arrays.asList(value._2.split(" ")).iterator(), Encoders.STRING())
            .toDF()
            .filter(col("value").startsWith("#"))
            .groupBy("value")
            .count()
            .writeStream()
            .outputMode("complete")
            .foreachBatch((batchDF, batchId) -> {
                batchDF.foreach((ForeachFunction<Row>) row -> writeToHBase(row.getString(0), row.getLong(1)));
            })
            .start()
            .awaitTermination();

    }
    public static void writeToHBase(String hashtag, long count) {
    Configuration config = HBaseConfiguration.create();
    try (Connection connection = ConnectionFactory.createConnection(config);
         Table table = connection.getTable(TableName.valueOf("trendingHashtagCount"))) {

        Put put = new Put(Bytes.toBytes(hashtag));
        put.addColumn(Bytes.toBytes("your_column_family"), Bytes.toBytes("count"), Bytes.toBytes(count));
        table.put(put);
    } catch (IOException e) {
        e.printStackTrace();
    }
}
}