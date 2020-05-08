package rabbitmq.pubsub.direct;

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
 * Il producer pubblica i file su un direct exchange,
 * associando ad ogni messaggio una routing key scelta a
 * caso da una lista.
 */
public class RoutingDirectProducerFile {

    private static final String EXCHANGE_NAME = "gb-exchange-direct-file";
    private static final String BROKER_IP = "localhost";

    public static void main(String[] argv) throws Exception {

        String[] routingKeys = {"aaaaa","bbbbb","ccccc","ddddd","eeeee"};

        // 1) connection to the broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(BROKER_IP);
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();

        // 2) create channel and direct exchange
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        // 3) publish to the exchange
        File fileDirectory = new File(System.getProperty("user.dir")+"\\files");
        File[] fileList = fileDirectory.listFiles();
        String routingKey;
        Random rand = new Random();
        for (File file : fileList) {
            FileMessage toSend = new FileMessage();
            toSend.setFilename(file.getName());
            toSend.setContent(Files.readAllBytes(Paths.get(file.getPath())));

            // La routing key viene scelta a caso fra quelle indicate sopra
            routingKey = routingKeys[rand.nextInt(routingKeys.length)];

            channel.basicPublish(EXCHANGE_NAME, routingKey,
                    null, SerializationUtils.serialize(toSend));

            System.out.println("Inviato il FileMessage: "+toSend+" con routing key: '"+routingKey+"'");

            Thread.sleep(3000);
        }

        channel.close();
        connection.close();
    }

}

