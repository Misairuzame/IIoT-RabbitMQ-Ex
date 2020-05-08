package rabbitmq.pubsub.file;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import org.apache.commons.lang3.SerializationUtils;
import rabbitmq.filemessage.FileMessage;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Classe vista a lezione leggermente modificata
 */
public class PublishFile {

    private static final String EXCHANGE_NAME = "gb-exchange-fanout-file";
    private static final String BROKER_IP = "localhost";

    public static void main(String[] argv) throws Exception {

        // 1) connection to the broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(BROKER_IP);
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();

        // 2) create channel and fanout exchange
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

        // 3) publish to the exchange
        File fileDirectory = new File(System.getProperty("user.dir")+"\\files");
        File[] fileList = fileDirectory.listFiles();
        for (File file : fileList) {
            FileMessage toSend = new FileMessage();
            toSend.setFilename(file.getName());
            toSend.setContent(Files.readAllBytes(Paths.get(file.getPath())));

            channel.basicPublish(EXCHANGE_NAME, "", null, SerializationUtils.serialize(toSend));

            System.out.println("Inviato il FileMessage: "+toSend);

            Thread.sleep(3000);
        }

        channel.close();
        connection.close();
    }

}

