package com.elsevier.cat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 * Wrapper for ArrayWritable
 * This is needed so we can persist the array to S3.
 * 
 * @author mcbeathd
 *
 */
public class StringArrayWritable extends ArrayWritable {
	
	// Logger
	private static Log log = LogFactory.getLog(StringArrayWritable.class);
	
	public StringArrayWritable() { super(Text.class); }
	
	public StringArrayWritable(String[] strings) { 
		super(Text.class);
		Text[] texts = new Text[strings.length];
	    for (int i = 0; i < strings.length; i++) {
	        texts[i] = new Text(strings[i]);
	    }
	    set(texts);
	}

}
