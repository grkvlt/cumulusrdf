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
import org.semanticweb.yars.nx.Resource;

import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.StoreException;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;

/** 
 * 
 * @author aharth
 */
@SuppressWarnings("serial")
public class CRUDServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		long start = System.currentTimeMillis();
			
		ServletContext ctx = getServletContext();

		if (req.getCharacterEncoding() == null)
				req.setCharacterEncoding("UTF-8");

		resp.setCharacterEncoding("UTF-8");
			
		String uri = req.getParameter("uri");

//			_log.info("req " + req.getPathInfo() + " " + req.getQueryString() + " " + s + " " + p + " " + o);

		if (uri == null) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
			
		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}

		int subjects = (Integer)ctx.getAttribute(Listener.TRIPLES_SUBJECT);
		int objects = (Integer)ctx.getAttribute(Listener.TRIPLES_OBJECT);
		
		Resource resource = new Resource(uri);
		
		Store crdf = (Store)ctx.getAttribute(Listener.STORE);
		PrintWriter out = resp.getWriter();
		int triples = 0;
		try {
			Iterator<Node[]> it = crdf.describe(resource, false, subjects, objects);
			if (it.hasNext()) {
				resp.setContentType(formatter.getContentType());
				triples = formatter.print(it, out);
			}
			else
				sendError(ctx, req, resp, HttpServletResponse.SC_NOT_FOUND, "resource not found");

		} 
		catch (StoreException e) {
			_log.severe(e.getMessage());
			sendError(ctx, req, resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
		}

		_log.info("[dataset] GET " + resource.toN3() + " " + (System.currentTimeMillis() - start) + "ms " + triples + " t");
	}
	
	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("POST currently not supported, sorry.");

//		String requestURI = req.getRequestURI();
//
//		// TODO for dataset mode we might want to handle POST on more than just the root URI
//		if (requestURI.equals(ROOT)) {
//			importFromInputStream(ctx, req, resp);
//		}
//		else {
//			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
//			return;
//		}
	}
	
	public void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("PUT currently not supported, sorry.");
	}
		
	public void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("DELETE currently not supported, sorry.");
	}
}