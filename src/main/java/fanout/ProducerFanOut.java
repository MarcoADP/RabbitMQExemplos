package fanout;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import utils.FactoryConnectionCreator;
import utils.Utils;

public class ProducerFanOut {

    private final static String EXCHANGE_NAME = "ex.example.fanout";

    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = FactoryConnectionCreator.create();

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            for (int i = 0; i <= 10; i++) {
                String message = String.format("Message #%d", i);
                channel.basicPublish(EXCHANGE_NAME,"", null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(Utils.getDateNow() + " => [x] Sent '" + message + "'");
                Thread.sleep(1000);
            }


        }
    }

}
