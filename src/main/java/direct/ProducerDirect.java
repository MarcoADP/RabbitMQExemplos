package direct;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import utils.FactoryConnectionCreator;
import utils.Utils;

public class ProducerDirect {

    private final static String EXCHANGE_NAME = "ex.example.direct";

    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = FactoryConnectionCreator.create();

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            // Sports
            for (int i = 0; i <= 5; i++) {
                String message = String.format("Message #%d about Sports", i);
                channel.basicPublish(EXCHANGE_NAME,"sports", null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(Utils.getDateNow() + " => [x] Sent '" + message + "'");
            }

            // Weather
            for (int i = 0; i <= 5; i++) {
                String message = String.format("Message #%d about Weather", i);
                channel.basicPublish(EXCHANGE_NAME,"weather", null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(Utils.getDateNow() + " => [x] Sent '" + message + "'");
            }

            // Finance
            for (int i = 0; i <= 5; i++) {
                String message = String.format("Message #%d about Finance", i);
                channel.basicPublish(EXCHANGE_NAME,"finance", null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(Utils.getDateNow() + " => [x] Sent '" + message + "'");
            }


        }
    }

}
