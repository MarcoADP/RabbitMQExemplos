package priorityqueue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import utils.FactoryConnectionCreator;
import utils.Utils;

public class ProducerTask {


    private final static String QUEUE_NAME = "priority_queue";
    private final static Integer MAX_PRIORITY = 255;


    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = FactoryConnectionCreator.create();

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {

            Map<String, Object> args = new HashMap<>();
            args.put("x-max-priority", MAX_PRIORITY);

            channel.queueDeclare(QUEUE_NAME, true, false, false, args);

            for (int i = 0; i <= 10; i++) {
                Integer priority = getPriority();
                String message = String.format("Message #%d with priority => #%d", i, priority);

                AMQP.BasicProperties properties = new AMQP.BasicProperties
                        .Builder()
                        .priority(priority)
                        .deliveryMode(2)
                        .build();

                channel.basicPublish(
                        "",
                        QUEUE_NAME,
                        properties,
                        message.getBytes(StandardCharsets.UTF_8)
                );
                System.out.println(Utils.getDateNow() + " => [x] Sent '" + message + "'");
            }


        }
    }

    private static int getPriority() {
        Random random = new Random();
        int min = 1;
        int max = MAX_PRIORITY;
        return random.nextInt(max - min) + min;
    }

}
