package dlx;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import utils.FactoryConnectionCreator;
import utils.Utils;

public class ConsumerDeadMessages {

    private final static String QUEUE_DLX_NAME = "q.dlx";
    private final static String EXCHANGE_DLX_NAME = "ex.example.dlx";

    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = FactoryConnectionCreator.create();

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_DLX_NAME, BuiltinExchangeType.DIRECT);

        Map<String, Object> args = new HashMap<>();
        String queueName = channel.queueDeclare(QUEUE_DLX_NAME, false, false, false, args).getQueue();
        channel.queueBind(queueName, EXCHANGE_DLX_NAME, "dlx");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(Utils.getDateNow() + " => [x] Received '" +
                    delivery.getEnvelope().getRoutingKey() + "':'" +  message + "'");
            System.out.println(delivery.getProperties());
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }

}
