package com.dima.hadoop.indexinverter;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TextWritableArray extends ArrayWritable {
    public TextWritableArray() {
        super(Text.class);
    }

    public TextWritableArray(Text[] values) {
        super(Text.class, values);
    }
    
    public void set(Text[] values) {
        super.set(values);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        boolean first = true;
        for (Writable item : super.get()) {
            if (!first)
                sb.append(", ");
            sb.append(item);
            first = false;
        }

        return sb.toString();
    }
}