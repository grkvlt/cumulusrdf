package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@SuppressWarnings("serial")
public class DispatcherServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	private static final String ROOT = "/";
	private static final String CRUD = "/crud";
	private static final String QUERY = "/query";
	private static final String SEARCH = "/search";
	private static final String SPARQL = "/sparql";
	
	private static final String INFO = "/info";
	private static final String ERROR = "/error";

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		ServletContext ctx = getServletContext();
//		_log.debug("serverName=" + req.getServerName() + " localName=" + req.getLocalName() + " Host=" + req.getHeader("Host") + " uri=" + req.getRequestURI() + " ctxpath=" + req.getContextPath());

		if (req.getServerName().equals(req.getLocalName())) {
			String requestURI = req.getRequestURI();
			String path = requestURI.substring(req.getContextPath().length());

			//_log.info("requestURI " + requestURI + " contextPath " + req.getContextPath());
			
			try {
				if (path.startsWith(CRUD)) {
					ctx.getNamedDispatcher("crud").forward(req, resp);
				} else if (path.startsWith(QUERY)) {
					ctx.getNamedDispatcher("query").forward(req, resp);
				}
				else if (path.startsWith(SPARQL)) {
					ctx.getNamedDispatcher("sparql").forward(req, resp);
				}
				else if (path.startsWith(ERROR)) {
					ctx.getNamedDispatcher("error").forward(req, resp);
				}
				else if (path.startsWith(INFO)) {
					ctx.getNamedDispatcher("info").forward(req, resp);
				}
				else if (path.startsWith(SEARCH)) {
					ctx.getNamedDispatcher("search").forward(req, resp);
				}
				else if (path.length() == 0) {
					resp.sendRedirect(requestURI + "/index.html");
				}
				else if (path.endsWith(ROOT)) {
					resp.sendRedirect(requestURI + "index.html");
				}
				else {
					ctx.getNamedDispatcher("default").forward(req, resp);
				}
			}
			catch (ServletException e) {
				e.printStackTrace();
				// FIXME sendError
				throw new IOException(e);
			}
		} else if ((Boolean)ctx.getAttribute(Listener.PROXY_MODE) == true) {
			ctx.getNamedDispatcher("proxy").forward(req, resp);
		} else {
			resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
			req.setAttribute("javax.servlet.error.status_code", HttpServletResponse.SC_NOT_FOUND);
			req.setAttribute("javax.servlet.error.message", "proxy mode disabled");
			try {
				ctx.getNamedDispatcher("error").include(req, resp);
			}
			catch (ServletException e) {
				// FIXME sendError
				e.printStackTrace();
			}
		}
	}
	
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		ServletContext ctx = getServletContext();
//		_log.debug("serverName=" + req.getServerName() + " localName=" + req.getLocalName() + " Host=" + req.getHeader("Host") + " uri=" + req.getRequestURI() + " ctxpath=" + req.getContextPath());

		if (req.getServerName().equals(req.getLocalName())) {
			String requestURI = req.getRequestURI();
			String path = requestURI.substring(req.getContextPath().length());

			//_log.info("requestURI " + requestURI + " contextPath " + req.getContextPath());
			
			try {
				if (path.startsWith(CRUD)) {
					ctx.getNamedDispatcher("crud").forward(req, resp);
				}
				else if (path.startsWith(SEARCH)) {
					ctx.getNamedDispatcher("search").forward(req, resp);
				}
				else {
					ctx.getNamedDispatcher("default").forward(req, resp);
				}
			}
			catch (ServletException e) {
				e.printStackTrace();
				// FIXME sendError
				throw new IOException(e);
			}
		} else if ((Boolean)ctx.getAttribute(Listener.PROXY_MODE) == true) {
			ctx.getNamedDispatcher("proxy").forward(req, resp);
		} else {
			resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
			req.setAttribute("javax.servlet.error.status_code", HttpServletResponse.SC_NOT_FOUND);
			req.setAttribute("javax.servlet.error.message", "proxy mode disabled");
			try {
				ctx.getNamedDispatcher("error").include(req, resp);
			}
			catch (ServletException e) {
				// FIXME sendError
				e.printStackTrace();
			}
		}
	}
}