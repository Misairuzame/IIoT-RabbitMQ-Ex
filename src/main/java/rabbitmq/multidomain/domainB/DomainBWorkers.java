package rabbitmq.multidomain.domainB;

import com.rabbitmq.client.*;
import org.apache.commons.lang3.SerializationUtils;
import rabbitmq.filemessage.FileMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

public class DomainBWorkers {

    public static void main(String[] args) throws Exception {
        worker("one");
        worker("two");
    }

    private static final String DOMAIN_B_QUEUE_NAME = "gb-domainb-workqueue-file";
    private static final String BROKER_IP = "localhost";

    public static void worker(String name) throws Exception {

        final String randomFolder = "000-domainB-"+name+"-"+ UUID.randomUUID().toString();

        // 1) connection to the broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(BROKER_IP);
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();

        // 2) create a channel
        Channel channel = connection.createChannel();
        channel.queueDeclare(DOMAIN_B_QUEUE_NAME, false, false, false, null);

        System.out.println(" [*] Waiting for messages on private queue '"+DOMAIN_B_QUEUE_NAME+"'. To exit press CTRL+C");

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
        channel.basicConsume(DOMAIN_B_QUEUE_NAME, true, consumer);
    }

}
