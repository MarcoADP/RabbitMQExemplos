package dlx;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import utils.FactoryConnectionCreator;
import utils.Utils;

public class Consumer {

    private final static String QUEUE_NAME = "q.normal";
    private final static String EXCHANGE_NAME = "ex.example.direct";
    private final static String EXCHANGE_DLX_NAME = "ex.example.dlx";

    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = FactoryConnectionCreator.create();

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", EXCHANGE_DLX_NAME);
        args.put("x-dead-letter-routing-key", "dlx");
        String queueName = channel.queueDeclare(QUEUE_NAME, false, false, false, args).getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "sports");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        /* */
        while (true) {
            GetResponse response = channel.basicGet(queueName, false);
            if (response != null) {
                String message = new String(response.getBody(), StandardCharsets.UTF_8);
                System.out.println(Utils.getDateNow() + " => [x] Received '" +
                        response.getEnvelope().getRoutingKey() + "':'" +  message + "'");
                // channel.basicNack(response.getEnvelope().getDeliveryTag(), false, false);
                channel.basicReject(response.getEnvelope().getDeliveryTag(), false);
            }
            Thread.sleep(5000);
        }

        /* */
    }

}
