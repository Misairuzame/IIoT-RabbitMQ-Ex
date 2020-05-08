package rabbitmq.pubsub.topic;

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
 * Consumer che si iscrive al topic exchange.
 * Ogni consumer riceverà solo i messaggi relativi
 * ai topic a cui si è iscritto. In questo caso ogni
 * consumer si iscrive a 1 o 2 topic.
 */
public class TopicConsumerFile {

    private static final String EXCHANGE_NAME = "gb-exchange-topic-file";
    private static final String BROKER_IP = "localhost";

    public static void main(String[] argv) throws Exception {

        final String randomFolder = "000-"+ UUID.randomUUID().toString();
        String[] topics = {"*.aaa","#.bbb","ccc.*","ddd.#","#"};

        // 1) connection to the broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(BROKER_IP);
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();

        // 2) create a channel,
        // a topic exchange from the channel,
        // and finally bind a queue to the exchange
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        String queueName = channel.queueDeclare().getQueue();
        // I topic vengono scelti a caso fra quelli indicati sopra,
        // per iscrivermi a più topic devo fare più channel.queueBind().
        String topic1 = topics[new Random().nextInt(topics.length)];
        channel.queueBind(queueName, EXCHANGE_NAME, topic1);
        String topic2 = topics[new Random().nextInt(topics.length)];
        channel.queueBind(queueName, EXCHANGE_NAME, topic2);

        System.out.println(" [*] Waiting for messages on private queue '"+queueName+"'\n" +
                "with topics '"+topic1+"','"+topic2+"'. To exit press CTRL+C");

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


