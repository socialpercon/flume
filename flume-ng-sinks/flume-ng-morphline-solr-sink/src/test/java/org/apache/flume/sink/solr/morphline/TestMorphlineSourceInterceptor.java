/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.solr.morphline;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.morphline.base.Fields;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

public class TestMorphlineSourceInterceptor extends Assert {

	private static final String RESOURCES_DIR = "target/test-classes";

	  @Test
	  public void testGrokIfNotMatchDropEventRetain() throws Exception {
	    Context context = new Context();
	    context.put(MorphlineHandlerImpl.MORPHLINE_FILE_PARAM, RESOURCES_DIR + "/test-morphlines/grokTest.conf");
	    //context.put(MorphlineHandlerImpl.MORPHLINE_VARIABLE_PARAM + ".MY.MIME_TYPE", "avro/binary");
	    
	    String msg = "[2014/01/22 17:02:17] 27950, \"DLD.py\", 849, FIND_NODE : TABLE[LOADING_TEST_32],key[K52],partition[20140101000000],IP[192.168.0.171]";
	    Event input = EventBuilder.withBody(null, ImmutableMap.of(Fields.MESSAGE, msg));
	    Event actual = build(context).intercept(input);

	    Map<String, String> expected = new HashMap();
	    expected.put(Fields.MESSAGE, msg);
	    expected.put("syslog_pri", "164");
	    expected.put("syslog_timestamp", "Feb  4 10:46:14");
	    expected.put("syslog_hostname", "syslog");
	    expected.put("syslog_program", "sshd");
	    expected.put("syslog_pid", "607");
	    expected.put("syslog_message", "Server listening on 0.0.0.0 port 22.");
	    Event expectedEvent = EventBuilder.withBody(null, expected);
	    //assertEqualsEvent(expectedEvent, actual);
	  }

	private MorphlineInterceptor build(Context context) {
		MorphlineInterceptor.Builder builder = new MorphlineInterceptor.Builder();
		builder.configure(context);
		return builder.build();
	}

	private void assertEqualsEvent(Event x, Event y) { // b/c SimpleEvent
														// doesn't implement
														// equals() method :-(
		assertEquals(x.getHeaders(), y.getHeaders());
		assertArrayEquals(x.getBody(), y.getBody());
	}

	private void assertEqualsEventList(List<Event> x, List<Event> y) {
		assertEquals(x.size(), y.size());
		for (int i = 0; i < x.size(); i++) {
			assertEqualsEvent(x.get(i), y.get(i));
		}
	}

}
