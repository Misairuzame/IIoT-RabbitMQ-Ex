package rabbitmq.multidomain.domainA;

import com.rabbitmq.client.*;
import org.apache.commons.lang3.SerializationUtils;
import rabbitmq.filemessage.FileMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

public class DomainAWorkers {

    public static void main(String[] args) throws Exception {
        worker();
        worker();
    }

    private static final String DOMAIN_A_PUBSUB = "gb-domaina-pubsub-file";
    private static final String BROKER_IP = "localhost";

    public static void worker() throws Exception {

        final String randomFolder = "000-domainA-"+ UUID.randomUUID().toString();

        // 1) connection to the broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(BROKER_IP);
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();

        // 2) create a channel,
        // a fanout exchange from the channel,
        // and finally bind a queue to the exchange
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(DOMAIN_A_PUBSUB, BuiltinExchangeType.FANOUT);
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, DOMAIN_A_PUBSUB, "");

        System.out.println(" [*] Waiting for messages on private queue '"+queueName+"'. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                FileMessage received = SerializationUtils.deserialize(body);
                System.out.println(this.hashCode()+" ha ricevuto: " + received);
                File randFoldFile = new File(randomFolder);
                randFoldFile.mkdir();
                String fileName = System.getProperty("user.dir") + "\\" + randomFolder + "\\" + received.getFilename();
                Files.write(Paths.get(fileName), received.getContent(), StandardOpenOption.CREATE);
                System.out.println("Salvato il file " + fileName);
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }

}
