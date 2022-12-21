package consistenthash;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import utils.FactoryConnectionCreator;
import utils.Utils;

public class ConsumerHash {

    private final static String EXCHANGE_NAME = "ex.example.hash";

    public static void main(String[] argv) throws Exception {

        final int[] received = {0};
        ConnectionFactory factory = FactoryConnectionCreator.create();

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "x-consistent-hash");

        String queueName = channel.queueDeclare().getQueue();
        String weight = String.valueOf(getWeight());
        channel.queueBind(queueName, EXCHANGE_NAME, weight);

        System.out.printf("Created Consumer with weight #%s%n", weight);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            received[0] = received[0] + 1;
            System.out.println(Utils.getDateNow() + " => [x] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" +  message + "'" + " -- Total #" + received[0]);
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }

    private static int getWeight() {
        Random random = new Random();
        int min = 1;
        int max = 5;
        return random.nextInt(max - min) + min;
    }

}
