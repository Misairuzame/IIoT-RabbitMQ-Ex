package rabbitmq.multidomain.domainA;

import com.rabbitmq.client.*;
import org.apache.commons.lang3.SerializationUtils;
import rabbitmq.filemessage.FileMessage;

import java.io.IOException;

public class ManagerA {

    private static final String EXCHANGE_NAME = "gb-multidomain-exchange-fanout-file";
    private static final String BROKER_IP = "localhost";
    private static final String DOMAIN_A_PUBSUB = "gb-domaina-pubsub-file";

    public static void main(String[] argv) throws Exception {

        // 1) connection to the broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(BROKER_IP);
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();

        // 2) create a channel,
        // a fanout exchange from the channel,
        // and finally bind a queue to the exchange
        Channel inChannel = connection.createChannel();
        inChannel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        String inQueueName = inChannel.queueDeclare().getQueue();
        inChannel.queueBind(inQueueName, EXCHANGE_NAME, "");

        // Ora creo connessione e canale in uscita
        Channel outChannel = connection.createChannel();
        outChannel.exchangeDeclare(DOMAIN_A_PUBSUB, BuiltinExchangeType.FANOUT);
        String outQueueName = outChannel.queueDeclare().getQueue();
        outChannel.queueBind(outQueueName, DOMAIN_A_PUBSUB, "");

        System.out.println(" [*] Waiting for messages on private queue '"+inQueueName+"'. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(inChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                FileMessage received = SerializationUtils.deserialize(body);
                System.out.println("Il Manager A: '"+this.hashCode()+"' ha ricevuto: " + received);

                outChannel.basicPublish(DOMAIN_A_PUBSUB, "", null, SerializationUtils.serialize(received));

                System.out.println("Il Manager A ha inviato il FileMessage: "+received);
            }
        };
        inChannel.basicConsume(inQueueName, true, consumer);
    }

}
