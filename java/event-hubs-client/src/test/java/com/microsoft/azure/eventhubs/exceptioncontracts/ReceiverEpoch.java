package com.microsoft.azure.eventhubs.exceptioncontracts;

import java.util.concurrent.ExecutionException;
import org.junit.Assume;
import org.junit.Test;

import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.eventhubs.lib.*;
import com.microsoft.azure.servicebus.*;

public class ReceiverEpoch extends TestBase
{

	@Test (expected = ReceiverDisconnectedException.class)
	public void testEpochReceiver() throws Throwable
	{
		Assume.assumeTrue(TestBase.isServiceRun());
		
		TestEventHubInfo eventHubInfo = TestBase.checkoutTestEventHub();
		try 
		{
			ConnectionStringBuilder connectionString = TestBase.getConnectionString(eventHubInfo);
			EventHubClient ehClient = EventHubClient.createFromConnectionString(connectionString.toString()).get();		
			
			try
			{
				String cgName = eventHubInfo.getRandomConsumerGroup();
				String partitionId = "0";
				long epoch = 345632;
				PartitionReceiver receiver = ehClient.createEpochReceiver(cgName, partitionId, PartitionReceiver.StartOfStream, false, epoch).get();
				receiver.setReceiveHandler(new EventCounter());
				
				try
				{
					ehClient.createEpochReceiver(cgName, partitionId, epoch - 10).get();
				}
				catch(ExecutionException exp)
				{
					throw exp.getCause();
				}
			}
			finally
			{
				ehClient.close();
			}
		}
		finally
		{
			TestBase.checkinTestEventHub(eventHubInfo.getName());
		}
	}

	public static final class EventCounter extends PartitionReceiveHandler
	{
		private long count;
		
		public EventCounter()
		{ 
			count = 0;
		}

		@Override
		public void onReceive(Iterable<EventData> events)
		{
			for(EventData event: events)
			{
				System.out.println(String.format("Counter: %s, Offset: %s, SeqNo: %s, EnqueueTime: %s, PKey: %s", 
						 this.count, event.getSystemProperties().getOffset(), event.getSystemProperties().getSequenceNumber(), event.getSystemProperties().getEnqueuedTime(), event.getSystemProperties().getPartitionKey()));
			}		
			
			count++;			
		}

		@Override
		public void onError(Exception exception)
		{	
		}
		
	}
}
