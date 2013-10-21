package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** 
 * @author aharth
 */
@SuppressWarnings("serial")
public abstract class AbstractHttpServlet extends HttpServlet {

	protected void sendError(ServletContext ctx, HttpServletRequest req, HttpServletResponse resp, int sc, String msg) {
		resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
		req.setAttribute("javax.servlet.error.status_code", HttpServletResponse.SC_NOT_FOUND);
		req.setAttribute("javax.servlet.error.message", msg);
		try {
			ctx.getNamedDispatcher("error").include(req, resp);
		} catch (ServletException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}