package takanorig.example.flink.datastream;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * WordCount with Windowing by Apache Flink.
 * 
 * Before executing this application, start the input stream with netcat from a terminal:
 * <pre>
 * $ nc -lk 9999
 * word1 word2 word3
 * word2 word3
 * word1
 * </pre>
 */
public class WindowWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> counts = env.socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                // Key by Index-0 element of a Tuple (word)
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                // Sum by Index-1 element of a Tuple (num)
                .sum(1);

        counts.print();

        env.execute("Window WordCount");
    }

    private static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            Arrays.stream(sentence.split(" ")).forEach(word -> out.collect(new Tuple2<String, Integer>(word, 1)));
        }
    }
}
