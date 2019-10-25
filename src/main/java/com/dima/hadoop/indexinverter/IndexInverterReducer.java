package com.dima.hadoop.indexinverter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Merges for each word the duplicate (word, docID) pairs and emits (word, list(docID))
 * where the list consists of all documents in which the word is contained.
 */
public class IndexInverterReducer extends Reducer<Text, Text, Text, TextWritableArray> {
	// Output array that will be written by the reduce task
	private TextWritableArray outArray = new TextWritableArray();
	
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // TODO: Eliminate duplicate entries, convert to Text[] array and feed outArray with it
        Set<Text> output = new HashSet<Text>();
        for (Text val : values) {
            // Create new Text object to store value. Reason: The Iterable<Text> reuses the
            // same Text object and just populates it with another value. Thus, you cannot
            // take just "val" as element of the HashSet, you need to duplicate it.
            Text elem = new Text(val);
            output.add(elem);
        }
        
        outArray.set(output.toArray(new Text[output.size()]));
        context.write(key, outArray);
    }
}
