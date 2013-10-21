package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.kit.aifb.cumulus.store.Store;

/**
 * @author aharth
 */
@SuppressWarnings("serial")
public class InfoServlet extends HttpServlet {
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		PrintWriter out = resp.getWriter();

		resp.setContentType("text/plain");

		ServletContext ctx = getServletContext();

		Store crdf = (Store)ctx.getAttribute(Listener.STORE);

		out.println(crdf.getStatus());

		if (ctx.getAttribute(Listener.PROXY_MODE) != null) {
			out.println("proxy mode enabled");
		}

		out.close();
	}
}