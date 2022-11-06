package br.com.gabriel;

import br.com.gabriel.dispatcher.KafkaDispatcher;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class GenerateAllReportsServlet extends HttpServlet {

    private final KafkaDispatcher<String> batchKafkaDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        batchKafkaDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
        try {
            batchKafkaDispatcher.send("ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS", "ECOMMERCE_USER_GENERATE_READING_REPORT",
                    new CorrelationId(GenerateAllReportsServlet.class.getSimpleName()),
                    "ECOMMERCE_USER_GENERATE_READING_REPORT");

            System.out.println("Sent generated reports");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New reports sent");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
