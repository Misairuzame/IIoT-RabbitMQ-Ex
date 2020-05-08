package rabbitmq.sendrecv.loadbalancing;


import com.rabbitmq.client.*;
import org.apache.commons.lang3.SerializationUtils;
import rabbitmq.filemessage.FileMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

/**
 * Ricezione di diversi file (in questo caso immagini),
 * ogni immagine inviata da SendFile viene ricevuta da
 * un solo consumer. Questa classe permette il load balancing,
 * ponendo basicQos a 1 e autoAck a false.
 */
public class RecvFileLoadBalancing {

    private static final String QUEUE_NAME = "gbqueuefile";
    private static final String BROKER_IP = "localhost";

    public static void main(String[] argv) {
        try {

            final String randomFolder = "000-"+ UUID.randomUUID().toString();

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(BROKER_IP);
            factory.setUsername("guest");
            factory.setPassword("guest");
            Connection connection = factory.newConnection();

            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.basicQos(1);

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

            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
            channel.basicConsume(QUEUE_NAME, false, consumer);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }
}
