package be.uclouvain.gepiciad.micro.alloy.nexmark.generators;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.admin.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

public class Q3Generator {
    private static String bootstrapServers;
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ParameterTool pt = ParameterTool.fromArgs(args);
        bootstrapServers = pt.get("bootstrapServers", "http://localhost:9092");
        String topic = pt.get("topic", "auctions");

        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createAuctionSourceTable =
                "CREATE TABLE auctions_q3 (\n"
                        + "id BIGINT,\n"
                        + "seller BIGINT\n"
                        + ") WITH ( \n"
                        + "'connector' = 'kafka', \n"
                        + "'properties.bootstrap.servers' = '" + bootstrapServers + "',\n"
                        + "'topic' = 'auctions_q3',\n"
                        + "'properties.group.id' = 'nexmark',\n"
                        + "'scan.startup.mode' = 'earliest-offset',\n" // earliest-offset
                        + "'key.format' = 'raw',\n"
                        + "'key.fields' = 'id', \n"
                        + "'value.format' = 'json')";
        tableEnv.executeSql(createAuctionSourceTable);

        String createPersonSourceTable =
                "CREATE TABLE persons_q3 (\n" +
                "    id  BIGINT,\n" +
                "    name  VARCHAR,\n" +
                "    city  VARCHAR,\n" +
                "    state  VARCHAR\n"
                        + ") WITH ( \n"
                        + "'connector' = 'kafka', \n"
                        + "'properties.bootstrap.servers' = '" + bootstrapServers + "',\n"
                        + "'topic' = 'persons_q3',\n"
                        + "'properties.group.id' = 'nexmark',\n"
                        + "'scan.startup.mode' = 'earliest-offset',\n" // earliest-offset
                        + "'key.format' = 'raw',\n"
                        + "'key.fields' = 'id', \n"
                        + "'value.format' = 'json')";
        tableEnv.executeSql(createPersonSourceTable);

        String createAuctionSinkTable =
                "CREATE TABLE auctions_q3p (\n"
                        + "id BIGINT,\n"
                        + "seller BIGINT,\n"
                        + "PRIMARY KEY (seller) NOT ENFORCED\n"
                        + ") WITH ( \n"
                        + "'connector' = 'upsert-kafka', \n"
                        + "'properties.bootstrap.servers' = '" + bootstrapServers + "',\n"
                        + "'topic' = 'auctions_q3p',\n"
                        + "'key.format' = 'raw',\n"
                        //+ "'key.fields' = 'label', \n"
                        + "'value.format' = 'json')";
        tableEnv.executeSql(createAuctionSinkTable);

        String createPersonSinkTable =
                "CREATE TABLE persons_q3p (\n" +
                "    id  BIGINT,\n" +
                "    name  VARCHAR,\n" +
                "    city  VARCHAR,\n" +
                "    state  VARCHAR,\n"
                + "PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH ( \n"
                        + "'connector' = 'upsert-kafka', \n"
                        + "'properties.bootstrap.servers' = '" + bootstrapServers + "',\n"
                        + "'topic' = 'persons_q3p',\n"
                        + "'key.format' = 'raw',\n"
                        //+ "'key.fields' = 'label', \n"
                        + "'value.format' = 'json')";
        tableEnv.executeSql(createPersonSinkTable);

        Table t1 = tableEnv.sqlQuery("SELECT * FROM auctions_q3");
        Table t2 = tableEnv.sqlQuery("SELECT * FROM persons_q3");

        DataStream<Row> rowStream1 = tableEnv.toDataStream(t1);
        // filter based on quantity field
        DataStream<Row> aggStream1 = rowStream1.keyBy((in) -> in.getField("seller"));

        Schema schema1 = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("seller", DataTypes.BIGINT())
                .build();

        DataStream<Row> rowStream2 = tableEnv.toDataStream(t2);
        // filter based on quantity field
        DataStream<Row> aggStream2 = rowStream2.keyBy((in) -> in.getField("id"));

        Schema schema2 = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.VARCHAR(2147483647))
                .column("city", DataTypes.VARCHAR(2147483647))
                .column("state", DataTypes.VARCHAR(2147483647))
                .build();


        tableEnv.createTemporaryView("aggtable1", aggStream1, schema1);
        tableEnv.createTemporaryView("aggtable2", aggStream2, schema2);

        if (topic.compareTo("auctions") == 0) {
            tableEnv.executeSql(String.format("INSERT INTO auctions_q3p SELECT * FROM aggtable1"));
        } else  {
            tableEnv.executeSql(String.format("INSERT INTO persons_q3p SELECT * FROM aggtable2"));
        }
    }
}
