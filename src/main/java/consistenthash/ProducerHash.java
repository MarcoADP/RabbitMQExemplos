package consistenthash;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import utils.FactoryConnectionCreator;
import utils.Utils;

public class ProducerHash {

    private final static String EXCHANGE_NAME = "ex.example.hash";

    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = FactoryConnectionCreator.create();

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "x-consistent-hash");

            for (int i = 0; i < 1000; i++) {
                String message = String.format("Message #%d", i);
                channel.basicPublish(EXCHANGE_NAME,String.valueOf(i), null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(Utils.getDateNow() + " => [x] Sent '" + message + "'");
            }
            System.out.println("Done!");


        }
    }

}
