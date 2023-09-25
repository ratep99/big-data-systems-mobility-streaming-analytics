package project;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.log4j.Logger;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;

public final class CassandraService {

    private static final Logger LOGGER = Logger.getLogger(DataStreamJob.class);

    public final void sinkToDatabase15(final DataStream<Tuple5<String, Double, Double,
                    Double, Integer>> sinkFilteredStream) throws Exception {
        LOGGER.info("Creating vehicle data to sink into cassandraDB.");

        sinkFilteredStream.print();
        LOGGER.info("Open Cassandra connection and Sinking bus data into cassandraDB.");
        CassandraSink
                .addSink(sinkFilteredStream)
                .setHost("cassandra")
                .setQuery("INSERT INTO flink_keyspace.statistika(id, min_speed, max_speed, mean_speed, count) " +
                        "values (?, ?, ?, ?, ?);")
                .build();
    }
    public final void sinkToDatabase30(final DataStream<Tuple1<String>> sinkFilteredStream) throws Exception {
        LOGGER.info("Creating car data to sink into cassandraDB.");

        sinkFilteredStream.print();
        LOGGER.info("Open Cassandra connection and Sinking bus data into cassandraDB.");
        CassandraSink
                .addSink(sinkFilteredStream)
                .setHost("cassandra")
                .setQuery("INSERT INTO flink_keyspace.top_locations(latitude_longitude_count) values (?);")
                .build();
    }
}
