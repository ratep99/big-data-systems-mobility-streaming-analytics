package project;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.log4j.Logger;

public final class CassandraService {

    private static final Logger logger = Logger.getLogger(CassandraService.class);

    public static void sinkToDatabase15(final DataStream<Tuple5<String, Double, Double, Double, Integer>> sinkFilteredStream) throws Exception {
        sinkFilteredStream.print();
        CassandraSink
                .addSink(sinkFilteredStream)
                .setHost("cassandra")
                .setQuery("INSERT INTO flink_keyspace.statistika(id, min_speed, max_speed, mean_speed, count) " +
                        "values (?, ?, ?, ?, ?);")
                .build();
    }

    public static void sinkToDatabase30(final DataStream<Tuple1<String>> sinkFilteredStream) throws Exception {
        sinkFilteredStream.print();
        CassandraSink
                .addSink(sinkFilteredStream)
                .setHost("cassandra")
                .setQuery("INSERT INTO flink_keyspace.top_locations(latitude_longitude_count) values (?);")
                .build();
    }
}
