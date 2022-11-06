package br.com.gabriel;

import br.com.gabriel.dispatcher.KafkaDispatcher;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.math.BigDecimal;
import java.util.UUID;

public class OrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderKafkaDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderKafkaDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
        try {
            var key = UUID.randomUUID().toString();
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));

            var orderValue = Order.builder()
                    .orderId(key)
                    .userId(UUID.randomUUID().toString())
                    .value(amount)
                    .email(email)
                    .build();
            orderKafkaDispatcher.send("ECOMMERCE_NEW_ORDER", key, new CorrelationId(OrderServlet.class.getSimpleName()), orderValue);

            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New order sent");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
