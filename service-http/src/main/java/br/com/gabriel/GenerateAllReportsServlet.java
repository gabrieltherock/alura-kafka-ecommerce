package br.com.gabriel;

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
            batchKafkaDispatcher.send("SEND_MESSAGE_TO_ALL_USERS", "USER_GENERATE_READING_REPORT", "USER_GENERATE_READING_REPORT");

            System.out.println("Sent generated reports");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New reports sent");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
