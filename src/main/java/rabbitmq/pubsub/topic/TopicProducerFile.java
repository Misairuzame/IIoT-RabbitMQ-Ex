package rabbitmq.pubsub.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import org.apache.commons.lang3.SerializationUtils;
import rabbitmq.filemessage.FileMessage;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

/**
 * Il producer pubblica i file su un topic exchange,
 * associando ad ogni messaggio un topic scelto a
 * caso da una lista.
 */
public class TopicProducerFile {

    private static final String EXCHANGE_NAME = "gb-exchange-topic-file";
    private static final String BROKER_IP = "localhost";

    public static void main(String[] argv) throws Exception {

        String[] topics = {"aaa.bbb","bbb.ccc","ccc.ddd","ddd.eee","aaa.eee"};

        // 1) connection to the broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(BROKER_IP);
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();

        // 2) create channel and topic exchange
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        // 3) publish to the exchange
        File fileDirectory = new File(System.getProperty("user.dir")+"\\files");
        File[] fileList = fileDirectory.listFiles();
        String topic;
        Random rand = new Random();
        for (File file : fileList) {
            FileMessage toSend = new FileMessage();
            toSend.setFilename(file.getName());
            toSend.setContent(Files.readAllBytes(Paths.get(file.getPath())));

            // Il topic viene scelto a caso fra quelli indicati sopra
            topic = topics[rand.nextInt(topics.length)];

            channel.basicPublish(EXCHANGE_NAME, topic,
                    null, SerializationUtils.serialize(toSend));

            System.out.println("Inviato il FileMessage: "+toSend+" con topic: '"+topic+"'");

            Thread.sleep(3000);
        }

        channel.close();
        connection.close();
    }

}

