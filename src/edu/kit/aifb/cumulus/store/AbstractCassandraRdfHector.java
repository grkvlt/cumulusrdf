package edu.kit.aifb.cumulus.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import me.prettyprint.cassandra.connection.LeastActiveBalancingPolicy;
import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars2.rdfxml.RDFXMLParser;

public abstract class AbstractCassandraRdfHector extends Store {
	protected class LoadThread extends Thread {

		private BlockingQueue<List<Node[]>> m_queue;
		private String m_cf;
		private boolean m_finished;
		private int m_id;
		
		public LoadThread(String columnFamily, int id) {
			m_cf = columnFamily;
			m_queue = new ArrayBlockingQueue<List<Node[]>>(5);
			m_id = id;
		}
		
		public void enqueue(List<Node[]> list) throws InterruptedException {
			m_queue.put(list);
		}
		
		public void setFinished(boolean finished) {
			m_finished = finished;
		}
		
		@Override
		public void run() {
			while (!m_finished || !m_queue.isEmpty()) {
				List<Node[]> list = null;
				try {
					list = m_queue.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
//				long start = System.currentTimeMillis();
				int tries = 10;
				while (tries >= 0) {
					try {
						batchInsert(m_cf, list);
						tries = -1;
					}
					catch (Exception e) {
						_log.severe("caught " + e + " while inserting into " + m_cf + " " + list.size() + " [" + m_id + ", tries left: " + tries + "]" + e.getMessage());
						e.printStackTrace();
						tries--;
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
					}
				}
//				_log.debug("[" + m_id + "] inserted " + list.size() + " in " + (System.currentTimeMillis() - start));
			}
		}
		
	}

	protected static final String DEFAULT_KS = "KeyspaceCumulus";
	
	protected static final String COL_S = "s";
	protected static final String COL_P = "p";
	protected static final String COL_O = "o";
	
	private final Logger _log = Logger.getLogger(this.getClass().getName());
	
	protected List<String> _cfs;
	protected Set<String> _cols;
	protected Map<String,int[]> _maps;
	protected String _hosts;
	protected Cluster _cluster;
	protected String _keyspaceName;
	protected Keyspace _keyspace;
	protected int _batchSizeMB = 1;
	protected StringSerializer _ss = StringSerializer.get();
	protected BytesArraySerializer _bs = BytesArraySerializer.get();

	protected AbstractCassandraRdfHector(String hosts) {
		this(hosts, DEFAULT_KS);
	}
	
	protected AbstractCassandraRdfHector(String hosts, String keyspace) {
		_keyspaceName = keyspace;
		_hosts = hosts;
		_maps = new HashMap<String,int[]>();
		_cfs = new ArrayList<String>();
		_cols = new HashSet<String>();
		_cols.add(COL_S);
		_cols.add(COL_P);
		_cols.add(COL_O);
		_log.info("cassandrardfhector class: " + getClass().getCanonicalName());
	}

	@Override
	public void open() throws StoreException {
		CassandraHostConfigurator config = new CassandraHostConfigurator(_hosts);
		config.setCassandraThriftSocketTimeout(60*1000);
//		config.setMaxActive(6);
//		config.setExhaustedPolicy(ExhaustedPolicy.WHEN_EXHAUSTED_BLOCK);
		config.setRetryDownedHostsDelayInSeconds(5);
		config.setRetryDownedHostsQueueSize(128);
		config.setRetryDownedHosts(true);
		
		// experiments with timeouts
		config.setCassandraThriftSocketTimeout(0);
		config.setMaxWaitTimeWhenExhausted(-1);
		
//		config.setLoadBalancingPolicy(new RoundRobinBalancingPolicy());
		config.setLoadBalancingPolicy(new LeastActiveBalancingPolicy());
		_cluster = HFactory.getOrCreateCluster("CassandraRdfHectorHierHash", config);
		
		boolean found = false;
		for (KeyspaceDefinition ksDef : _cluster.describeKeyspaces()) {
			if (ksDef.getName().equals(_keyspaceName)) {
				found = true;
				break;
			}
		}
		
		if (!found)
			_cluster.addKeyspace(createKeyspaceDefinition());
		
		_keyspace = HFactory.createKeyspace(_keyspaceName, _cluster, new ConsistencyLevelPolicy() {
			@Override
			public HConsistencyLevel get(OperationType arg0, String arg1) {
				return HConsistencyLevel.ONE;
			}
			
			@Override
			public HConsistencyLevel get(OperationType arg0) {
				return HConsistencyLevel.ONE;
			}
		});
		_log.finer("connected to " + _hosts);
	}
	
	protected ColumnDefinition createColDef(String colName, String validationClass, boolean indexed) {
		return createColDef(colName, validationClass, indexed, colName + "_index");
	}
	
	protected ColumnDefinition createColDef(String colName, String validationClass, boolean indexed, String indexName) {
		BasicColumnDefinition colDef = new BasicColumnDefinition();
		colDef.setName(_ss.toByteBuffer(colName));
		colDef.setValidationClass(validationClass);
		if (indexed) {
			colDef.setIndexType(ColumnIndexType.KEYS);
			colDef.setIndexName(indexName);
		}
		return colDef;
	}

	public void setBatchSize(int batchSizeMB) {
		_batchSizeMB = batchSizeMB;
	}
	
	protected KeyspaceDefinition createKeyspaceDefinition() {
		return HFactory.createKeyspaceDefinition(_keyspaceName, "org.apache.cassandra.locator.SimpleStrategy", 1, createColumnFamiliyDefinitions());
	}

	protected abstract List<ColumnFamilyDefinition> createColumnFamiliyDefinitions();

	protected ColumnFamilyDefinition createCfDefFlat(String cfName, List<String> cols, List<String> indexedCols, ComparatorType keyComp) {
		BasicColumnFamilyDefinition cfdef = new BasicColumnFamilyDefinition();
		cfdef.setKeyspaceName(_keyspaceName);
		cfdef.setName(cfName);
		cfdef.setColumnType(ColumnType.STANDARD);
		cfdef.setComparatorType(ComparatorType.UTF8TYPE);
		cfdef.setKeyValidationClass(keyComp.getClassName());
		cfdef.setDefaultValidationClass(ComparatorType.UTF8TYPE.getClassName());

		Map<String,String> compressionOptions = new HashMap<String, String>();
		compressionOptions.put("sstable_compression", "SnappyCompressor");
		cfdef.setCompressionOptions(compressionOptions);

		if (cols != null)
			for (String colName : cols)
				cfdef.addColumnDefinition(createColDef(colName, ComparatorType.UTF8TYPE.getClassName(), indexedCols.contains(colName), "index_" + colName.substring(1)));
		
		return new ThriftCfDef(cfdef);
	}
	
	protected ColumnFamilyDefinition createCfDefHier(String cfName) {
		BasicColumnFamilyDefinition cfdef = new BasicColumnFamilyDefinition();
		cfdef.setKeyspaceName(_keyspaceName);
		cfdef.setName(cfName);
		cfdef.setColumnType(ColumnType.SUPER);
		cfdef.setComparatorType(ComparatorType.UTF8TYPE);
		Map<String,String> compressionOptions = new HashMap<String, String>();
		compressionOptions.put("sstable_compression", "SnappyCompressor");
		cfdef.setCompressionOptions(compressionOptions);
		return new ThriftCfDef(cfdef);
	}
	
	protected abstract void batchInsert(String cf, List<Node[]> li);

	protected boolean isVariable(Node n) {
		return n instanceof Variable;
	}

	@Override
	public void close() throws StoreException {
		_cluster.getConnectionManager().shutdown();
	}

	@Override
	public int addData(Iterator<Node[]> it) throws StoreException {
		List<Node[]> batch = new ArrayList<Node[]>();
		int batchSize = 0;
		int count = 0;
		while (it.hasNext()) {
			Node[] nx = it.next();

			batch.add(nx);
			batchSize += nx[0].toN3().getBytes().length + nx[1].toN3().getBytes().length + nx[2].toN3().getBytes().length;
			count++;
			
			if (batchSize >= _batchSizeMB * 1048576) {
				_log.finer("insert batch of size " + batchSize + " (" + batch.size() + " tuples)");
				for (String cf : _cfs)
					batchInsert(cf, batch);
				batch = new ArrayList<Node[]>();
				batchSize = 0;
			}
		}
		
		if (batch.size() > 0)
			for (String cf : _cfs)
				batchInsert(cf, batch);
					
		return count;
	}

	@Override
	public boolean contains(Node s) throws StoreException {
		return query(new Node[] { s, new Variable("p"), new Variable("o") }).hasNext();
	}

	@Override
	public Iterator<Node[]> query(Node[] query) throws StoreException {
		return query(query, Integer.MAX_VALUE);
	}

	private Node urlDecode(Node n) {
		if (n instanceof Resource)
			try {
				return new Resource(URLDecoder.decode(n.toString(), "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				_log.severe(n.toN3() + " " + e.getMessage());
				return n;
			}
		return n;
	}

	public void batchBulkLoad(InputStream fis, String format, String columnFamily, int threadCount) throws IOException, InterruptedException {
		_log.info("bulk loading " + columnFamily);

		List<LoadThread> threads = new ArrayList<LoadThread>();
		
		if (threadCount < 0)
			threadCount = Math.max(1, (int)(_cluster.getConnectionManager().getHosts().size() / 1.5));

		for (int i = 0; i < threadCount; i++) {
			LoadThread t = new LoadThread(columnFamily, i);
			threads.add(t);
			t.start();
		}
		_log.info("created " + threads.size() + " loading threads");
		
		int curThread = 0;
		
		Iterator<Node[]> nxp = null;
		if (format.equals("nt") || format.equals("nq")) {
			nxp = new NxParser(fis);
		}
		else if (format.equals("xml")) {
			try {
				nxp = new RDFXMLParser(fis, "http://example.org");
			}
			catch (ParseException e) {
				e.printStackTrace();
				_log.severe(e.getMessage());
				throw new IOException(e);
			}
		}

		List<Node[]> triples = new ArrayList<Node[]>();
		
		long start = System.currentTimeMillis();
		int i = 0;
		int batchSize = 0;
		long data = 0;
		while (nxp.hasNext()) {
			Node[] nx = nxp.next();
			if (nx[2].toN3().length() + nx[1].toN3().length() > 64000) {
				_log.info("skipping too large row (max row size: 64k");
				continue;
			}
			
//			if (nx.length > 3)
//				nx = new Node[] { nx[0], nx[1], nx[2] };
			
//			nx[0] = urlDecode(nx[0]);
//			nx[1] = urlDecode(nx[1]);
//			nx[2] = urlDecode(nx[2]);
			
//			triples.add(Util.reorder(nx, map));
			triples.add(nx);
			
			i++;
			for (int k=0; k < nx.length; k++) {
				batchSize += nx[k].toN3().getBytes().length; // + nx[1].toN3().getBytes().length + nx[2].toN3().getBytes().length;
			}
			
			if (batchSize >= _batchSizeMB * 1048576) {
				_log.finer("batch ready: " + triples.size() + " triples, size: " + batchSize + ", thread: " + curThread);
				data += batchSize;
				threads.get(curThread).enqueue(triples);
				triples = new ArrayList<Node[]>();
				batchSize = 0;
				
				curThread = (curThread + 1) % threads.size();
			}
			
			if (i % 200000 == 0)
				_log.info(i + " into " + columnFamily + " in " +  (System.currentTimeMillis() - start) + " ms (" + ((double)i / (System.currentTimeMillis() - start) * 1000) + " triples/s) (" + ((double)data / 1000 / (System.currentTimeMillis() - start) * 1000) + " kbytes/s)");
		}
		
		if (triples.size() > 0) {
			threads.get(curThread).enqueue(triples);
		}
		
		_log.info("waiting for threads to finish....");
		for (LoadThread t : threads) {
			t.setFinished(true);
			t.enqueue(new ArrayList<Node[]>());
			t.join();
		}
		
		long time = (System.currentTimeMillis() - start);
		_log.info(i + " triples inserted into " + columnFamily + " in " + time + " ms (" + ((double)i / time * 1000) + " triples/s)");
	}

	public void bulkLoad(File file, String format) throws StoreException, IOException {
		bulkLoad(file, format, -1);
	}
	
	public void bulkLoad(File file, String format, int threads) throws StoreException, IOException {
		try {
			for (String cf : _cfs) {
				FileInputStream fis = new FileInputStream(file);
				batchBulkLoad(fis, format, cf, threads);
				fis.close();
			}			
		} catch (InterruptedException e) {
			throw new StoreException(e);
		} catch (IOException e) {
			throw new StoreException(e);
		}
	}

	public void bulkLoad(File file, String format, String cf) throws StoreException, IOException {
		bulkLoad(file, format, cf, -1);
	}

	public void bulkLoad(File file, String format, String cf, int threads) throws StoreException {
		try {
			FileInputStream fis = new FileInputStream(file);
			batchBulkLoad(fis, format, cf, threads);
			fis.close();
		} catch (InterruptedException e) {
			throw new StoreException(e);
		} catch (IOException e) {
			throw new StoreException(e);
		}
	}

	public void bulkLoad(InputStream is, String format, String cf) throws StoreException, IOException {
		bulkLoad(is, format, cf, -1);
	}

	public void bulkLoad(InputStream is, String format, String cf, int threads) throws StoreException {
		try {
			batchBulkLoad(is, format, cf, threads);
		} catch (InterruptedException e) {
			throw new StoreException(e);
		} catch (IOException e) {
			throw new StoreException(e);
		}
	}

	public String getStatus() {
		StringBuffer sb = new StringBuffer();
		
		sb.append("Connected to cluster: ");
		sb.append(_cluster.getConnectionManager().getClusterName());
		sb.append('\n');

		sb.append("Status per pool:\n");
		
		for (String s : _cluster.getConnectionManager().getStatusPerPool()) {
			sb.append(s);
			sb.append('\n');
		}
		
		return sb.toString();
	}
}
