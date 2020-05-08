package rabbitmq.sendrecv.basic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Classe vista a lezione leggermente modificata
 */
public class Send {

    private static final String QUEUE_NAME = "gbqueue";
    private static final String BROKER_IP = "localhost";

    public static void main(String[] argv) throws Exception {

        // 1) connection to the broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(BROKER_IP);
        //factory.setVirtualHost("/dsg_lab");
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();

        // 2) creation of channel and queue
    /*
	queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String,Object> arguments)
    queue - the name of the queue
	durable - true if we are declaring a durable queue (the queue will survive a server restart)
	exclusive - true if we are declaring an exclusive queue (restricted to this connection)
	autoDelete - true if we are declaring an autodelete queue (server will delete it when no longer in use)
	arguments - other properties (construction arguments) for the queue
     */
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // 3) publish of message
    /*
    https://rabbitmq.github.io/rabbitmq-java-client/api/current/com/rabbitmq/client/Channel.html
    void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) throws IOException
	exchange - the exchange to publish the message to
	routingKey - the routing key
	props - other properties for the message - routing headers etc
	body - the message body
     */
        String message = "Hello World!";
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));

        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}
