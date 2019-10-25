package com.dima.hadoop.indexinverter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Merges duplicate (word, docName) pairs. Since we do not want
 * to count the occurrences of a word in a document, it is sufficient to just
 * transmit one instance to the reducer.
 */
public class IndexInverterCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    	// TODO: Eliminate duplicate entries in values by only emitting
    	// a value if it has not been emitted before (in this combiner invocation)
        Set<Text> output = new HashSet<Text>();
        for (Text val : values) {
            Text elem = new Text(val);
            // check whether element has already been processed
            if (!output.contains(elem)) {
                context.write(key, elem);
                output.add(elem);
            }
        }
    }
}