package com.skt.samson.flume.channel;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.AbstractChannel;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuMemoryChannel extends AbstractChannel{
	
	private final Logger logger = LoggerFactory.getLogger(CuMemoryChannel.class);
	
	private LinkedBlockingQueue<Event> queue = new LinkedBlockingQueue<Event>(100000);
	
	private int capacity = 100000;
	
	private boolean isClosed = false;
	
	public CuMemoryChannel() {
		 super();
	}
	
	@Override
	public void configure(Context context) {
		super.configure(context);
		capacity = context.getInteger("capacity",  100000);
		queue = new LinkedBlockingQueue<Event>(capacity);
	}
	
	@Override
	public void put(Event event) throws ChannelException {
		if(isClosed){
			return;
		}
		try {
			queue.put(event);
		} catch (InterruptedException e) {
		}
		if(queue.size() > capacity && queue.size()%10000 == 0){
			logger.info("{}` queue Size {}", getName(), queue.size());
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
			}
		}
	}

	@Override
	public Event take() throws ChannelException {
		try {
			return queue.take();
		} catch (InterruptedException e) {
			return null;
		}
	}
	
	@Override
	public synchronized void stop() {
		super.stop();
		isClosed = true;
	}
	
	@Override
	public Transaction getTransaction() {
		return tx;
	}
	
	Transaction tx = new Transaction(){

		@Override
		public void begin() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void commit() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void rollback() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			
		}
		
	};
	
}
