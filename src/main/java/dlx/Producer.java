package dlx;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import utils.FactoryConnectionCreator;
import utils.Utils;

public class Producer {

    private final static String EXCHANGE_NAME = "ex.example.direct";

    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = FactoryConnectionCreator.create();

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            String message = "Mensagem rejeitada";
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .build();
            channel.basicPublish(EXCHANGE_NAME,"sports", replyProps, message.getBytes(StandardCharsets.UTF_8));

            System.out.println(Utils.getDateNow() + " => [x] Sent '" + message + "'");


        }
    }

}
