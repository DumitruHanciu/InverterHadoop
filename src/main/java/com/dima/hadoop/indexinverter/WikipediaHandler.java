package com.dima.hadoop.indexinverter;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class WikipediaHandler extends DefaultHandler {

	private String title = "";
	private String text = "";
	private StringBuilder sbTitle = new StringBuilder();
	private StringBuilder sbText = new StringBuilder();
	private boolean inTitle = false;
	private boolean inText = false;

	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {

		if (qName.equalsIgnoreCase("title")) {
			inTitle = true;
		} else if (qName.equalsIgnoreCase("text")) {
			inText = true;
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		if (qName.equalsIgnoreCase("title")) {
			title = sbTitle.toString();
			inTitle = false;
		} else if (qName.equalsIgnoreCase("text")) {
			text = sbText.toString();
			inText = false;
		}
	}

	@Override
	public void characters(char ch[], int start, int length) throws SAXException {
		if (inTitle) {
			sbTitle.append(new String(ch, start, length));
		} else if (inText) {
			sbText.append(new String(ch, start, length));
		}
	}
	
	public String getTitle() {
		return title;
	}

	public String getText() {
		return text;
	}
}