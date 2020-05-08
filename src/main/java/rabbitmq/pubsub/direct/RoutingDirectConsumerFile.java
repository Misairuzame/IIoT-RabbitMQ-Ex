package rabbitmq.pubsub.direct;

import com.rabbitmq.client.*;
import org.apache.commons.lang3.SerializationUtils;
import rabbitmq.filemessage.FileMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.UUID;

/**
 * Consumer che si iscrive al direct exchange.
 * Ogni consumer riceverà solo i messaggi relativi
 * alle routing keys che ha specificato.
 * In questo caso ogni consumer fa il binding di
 * 1 o 2 routing keys.
 */
public class RoutingDirectConsumerFile {

    private static final String EXCHANGE_NAME = "gb-exchange-direct-file";
    private static final String BROKER_IP = "localhost";

    public static void main(String[] argv) throws Exception {

        final String randomFolder = "000-"+ UUID.randomUUID().toString();
        String[] routingKeys = {"aaaaa","bbbbb","ccccc","ddddd","eeeee"};

        // 1) connection to the broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(BROKER_IP);
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();

        // 2) create a channel,
        // a direct exchange from the channel,
        // and finally bind a queue to the exchange
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = channel.queueDeclare().getQueue();
        // Le routing keys vengono scelte a caso fra quelle indicate sopra,
        // per fare il bind di più routing keys devo fare più channel.queueBind().
        String routingKey1 = routingKeys[new Random().nextInt(routingKeys.length)];
        channel.queueBind(queueName, EXCHANGE_NAME, routingKey1);
        String routingKey2 = routingKeys[new Random().nextInt(routingKeys.length)];
        channel.queueBind(queueName, EXCHANGE_NAME, routingKey2);

        System.out.println(" [*] Waiting for messages on private queue '"+queueName+"'\n" +
                "with routing keys '"+routingKey1+"','"+routingKey2+"'. To exit press CTRL+C");

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


