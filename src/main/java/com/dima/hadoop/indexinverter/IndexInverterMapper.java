package com.dima.hadoop.indexinverter;

import java.io.IOException;
import java.io.StringReader;
import java.util.StringTokenizer;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Maps each word in a given document to (word, docName) pairs.
 */
public class IndexInverterMapper extends Mapper<LongWritable, Text, Text, Text> {
    private SAXParser saxParser;
    // TODO: Create output key (word) and value (docName) objects of map function
    private Text word = new Text();
    private Text docName = new Text();

    @Override
    public void setup(Context context) {
        // Set up SAX parser
    	SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
    	try {
			saxParser = saxParserFactory.newSAXParser();
		} catch (ParserConfigurationException | SAXException e) {
			e.printStackTrace();
		}
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Each input of the map function contains one Wikipedia article, parse it and extract data
        WikipediaHandler saxHandler = new WikipediaHandler();
		try {
			saxParser.parse(new InputSource(new StringReader(value.toString())), saxHandler);
		} catch (SAXException e) {
			e.printStackTrace();
		}

		// Extract title and page contents of article
    	String title = saxHandler.getTitle();
    	String text = saxHandler.getText();
    	// Clean up text (remove special symbols)
    	text = text.replaceAll("[^A-Za-z0-9\\- ]", "");
    	// We do not care about case sensitivity
        text = text.toLowerCase().trim();

        // TODO: Split up text in words and emit for each word w (w, title), but do so only for words with a length
        // greater than 3
        StringTokenizer tokenizer = new StringTokenizer(text);
        while (tokenizer.hasMoreTokens()) {
            String nextToken = tokenizer.nextToken();
            if (nextToken.length() > 3) {
                word.set(nextToken);
                docName.set(title);
                context.write(word, docName);
            }
        }
    }
}