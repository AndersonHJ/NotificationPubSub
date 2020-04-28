package neu.shiqipubsub;

import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Map;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSubscriber;
import javax.jms.DeliveryMode;
import javax.jms.TopicSession;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.integration.transports.netty.NettyConnectorFactory;
import org.hornetq.integration.transports.netty.TransportConstants;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;

public class NotificationPublisher {
	
	static void startServer()
	{
		try
		  {
		     FileConfiguration configuration = new FileConfiguration();
		     configuration.setConfigurationUrl("hornetq-configuration.xml");
		     configuration.start();
		
		     HornetQServer server = HornetQServers.newHornetQServer(configuration);
		     JMSServerManager jmsServerManager = new JMSServerManagerImpl(server, "hornetq-jms.xml");
		     //if you want to use JNDI, simple inject a context here or don't call this method and make sure the JNDI parameters are set.
		     jmsServerManager.setContext(null);
		     jmsServerManager.start();
			 System.out.println("We can start publish messages !");
			 System.out.print("Please input the message to publish: \n");
		  }
		  catch (Throwable e)
		  {
		     System.out.println("Damn it !!");
		     e.printStackTrace();
		  }
	}
	
	public static void main(String[] args) throws Exception
	{
		//Start the server
		startServer();
		
		TopicConnection connection = null;
		try 
		{
			// Step 1. Directly instantiate the JMS Queue object.
			Topic topic = HornetQJMSClient.createTopic("mailboxTopic");

			// Step 2. Instantiate the TransportConfiguration object which
			// contains the knowledge of what transport to use,
			// The server port etc.
			Map<String, Object> connectionParams = new HashMap<String, Object>();
			connectionParams.put(TransportConstants.PORT_PROP_NAME, 5445);

			TransportConfiguration transportConfiguration = new TransportConfiguration(
																NettyConnectorFactory.class.getName(), connectionParams);

			// Step 3 Directly instantiate the JMS ConnectionFactory object
			// using that TransportConfiguration
			TopicConnectionFactory connFactory = (TopicConnectionFactory) HornetQJMSClient.createConnectionFactory(transportConfiguration);

			// Step 4.Create a JMS Connection
			connection = connFactory.createTopicConnection();

			TopicSession topicSession = connection.createTopicSession(false, 
			Session.AUTO_ACKNOWLEDGE);
																			
			// create a topic publisher
			TopicPublisher topicPublisher = topicSession.createPublisher(topic);
			topicPublisher.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
																				
			// create the "Hello World" message
            TextMessage message = topicSession.createTextMessage();

            BufferedReader reader =
                   new BufferedReader(new InputStreamReader(System.in));

            
            String line = reader.readLine();
            
            while(!line.equals("exit")){
                message.setText(line);
                                                                                    
                // publish the messages
                topicPublisher.publish(message);

                System.out.print("Please input the message to publish: \n");
                line = reader.readLine();
            }
            
            if(line.equals("exit")){
				System.out.println("exiting ...");
                return;
            }
		} 
		finally
		{
			if (connection != null) {
				connection.close();
			}
		}
	}
}
