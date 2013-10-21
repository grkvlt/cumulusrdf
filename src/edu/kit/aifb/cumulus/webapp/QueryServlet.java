package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.StoreException;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;

/**
 * @author aharth
 */
@SuppressWarnings("serial")
public class QueryServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		long start = System.currentTimeMillis();

		ServletContext ctx = getServletContext();

		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		
		int queryLimit = (Integer)ctx.getAttribute(Listener.QUERY_LIMIT);

		resp.setCharacterEncoding("UTF-8");

		String s = req.getParameter("s");
		String p = req.getParameter("p");
		String o = req.getParameter("o");

		//			_log.info("req " + req.getPathInfo() + " " + req.getQueryString() + " " + s + " " + p + " " + o);

		Node[] query = new Node[3];
		try {
			query[0] = getNode(s, "s");
			query[1] = getNode(p, "p");
			query[2] = getNode(o, "o");
		}
		catch (ParseException e) {
			_log.severe(e.getMessage());
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "could not parse query string");
			return;
		}

		if (query[0] instanceof Variable && query[1] instanceof Variable && query[2] instanceof Variable) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "query must contain at least one constant");
			return;
		}

		Store crdf = (Store)ctx.getAttribute(Listener.STORE);
		PrintWriter out = resp.getWriter();
		int triples = 0;
		try {
			Iterator<Node[]> it = crdf.query(query, queryLimit);
			if (it.hasNext()) {
				resp.setContentType(formatter.getContentType());
				triples = formatter.print(it, out);
			} else {
				sendError(ctx, req, resp, HttpServletResponse.SC_NOT_FOUND, "resource not found");
			}
		} catch (StoreException e) {
			_log.severe(e.getMessage());
			resp.sendError(500, e.getMessage());
		}
		
		_log.info("[dataset] QUERY " + Nodes.toN3(query) + " " + (System.currentTimeMillis() - start) + "ms " + triples + "t");
	}

	private Node getNode(String value, String varName) throws ParseException {
		if (value != null && value.trim().length() > 2) 
			return NxParser.parseNode(value);
		else
			return new Variable(varName);
	}
}