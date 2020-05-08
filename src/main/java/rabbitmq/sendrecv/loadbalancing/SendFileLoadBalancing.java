package rabbitmq.sendrecv.loadbalancing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.lang3.SerializationUtils;
import rabbitmq.filemessage.FileMessage;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Invio di diversi file (in questo caso immagini).
 * Supporto al load balancing tramite basicQos = 1
 * e autoAck nei consumer = false
 */
public class SendFileLoadBalancing {

    private static final String QUEUE_NAME = "gbqueuefile";
    private static final String BROKER_IP = "localhost";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(BROKER_IP);
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.basicQos(1);

        File fileDirectory = new File(System.getProperty("user.dir")+"\\files");
        File[] fileList = fileDirectory.listFiles();
        for (File file : fileList) {
            FileMessage toSend = new FileMessage();
            toSend.setFilename(file.getName());
            toSend.setContent(Files.readAllBytes(Paths.get(file.getPath())));

            channel.basicPublish("", QUEUE_NAME, null, SerializationUtils.serialize(toSend));

            System.out.println("Inviato il FileMessage: "+toSend);

            Thread.sleep(3000);
        }

        channel.close();
        connection.close();
    }
}
