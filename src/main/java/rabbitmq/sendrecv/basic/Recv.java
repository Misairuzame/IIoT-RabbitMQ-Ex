package rabbitmq.sendrecv.basic;


import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
/**
 * Classe vista a lezione leggermente modificata
 */
public class Recv {

    private static final String QUEUE_NAME = "gbqueue";
    private static final String BROKER_IP = "localhost";

    public static void main(String[] argv) throws Exception {

        // 1) connection to the broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(BROKER_IP);
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();

        // 2) creation of channel and queue
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // 3) implementation of a message handler/callback
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
            }
        };

        // 4) waiting for messages (together with handler registration)
    /*
    basicConsume(String queue, boolean autoAck, Consumer callback)
    queue - the name of the queue
	autoAck - true if the server should consider messages acknowledged once delivered; false if the server should expect explicit acknowledgments
	callback - an interface to the consumer object
     */
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }
}
