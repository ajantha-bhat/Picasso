/*
 # Copyright (C) 2005, 2006 Indian Institute of Science
 # Bangalore 560012, INDIA
 #
 # This program is part of the PICASSO distribution invented at the
 # Database Systems Lab, Indian Institute of Science. The use of
 # the software is governed by the licensing agreement set up between 
 # the owner, Indian Institute of Science, and the licensee.
 #
 # This program is distributed WITHOUT ANY WARRANTY; without even the 
 # implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  
 #
 # The public URL of the PICASSO project is
 # http://dsl.serc.iisc.ernet.in/projects/PICASSO/index.html
 #
 # For any issues, contact 
 #       Prof. Jayant R. Haritsa
 #       SERC
 #       Indian Institute of Science
 #       Bangalore 560012, India.
 #       Telephone: (+91) 80 2293-2793
 #       Fax      : (+91) 80 2360-2648
 #       Email: haritsa@dsl.serc.iisc.ernet.in
 #       WWW: http://dsl.serc.iisc.ernet.in/~haritsa
 */
package iisc.dsl.picasso.server.db.presto;

import iisc.dsl.picasso.common.PicassoConstants;
import iisc.dsl.picasso.common.ds.DBSettings;
import iisc.dsl.picasso.server.PicassoException;
import iisc.dsl.picasso.server.db.Database;
import iisc.dsl.picasso.server.db.Histogram;
import iisc.dsl.picasso.server.plan.Node;
import iisc.dsl.picasso.server.plan.Plan;
import iisc.dsl.picasso.server.network.ServerMessageUtil;

import java.math.BigDecimal;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PrestoDatabase extends Database 
{
	private int qno;
	public PrestoDatabase(DBSettings settings) throws PicassoException
	{
		super(settings);
		qno = 0;
	}
	
	public boolean connect(DBSettings settings) 
	{
		String connectString;
		if(isConnected())
				return true;
		this.settings = settings;
		try{
			connectString = "jdbc:presto://" + settings.getServerName() + ":" +	settings.getServerPort() + "/hive/" + settings.getDbName();
			//Register the JDBC driver for presto
			Class.forName("io.prestosql.jdbc.PrestoDriver").newInstance();
			//Get a connection to the database
			con = DriverManager.getConnection (connectString, "test", null);
			}
		catch (Exception e)	{ 
			ServerMessageUtil.SPrintToConsole("Database: " + e);
			return false;
			}
		/*if(con != null) {
			if( !(settings.getOptLevel().trim().equalsIgnoreCase("Default"))) {
				try	{
					Statement stmt = createStatement();
					String optLevelQuery ="set session OPTIMIZER_MODE = "+settings.getOptLevel();
					stmt.execute(optLevelQuery);
					}
				catch(SQLException se) {
					ServerMessageUtil.SPrintToConsole("Database : Error setting the Optimization Level of presto : "+se);
					}
			}
			else {
				try	{
					Statement stmt = createStatement();
					String optLevelQuery ="set session OPTIMIZER_MODE = "+settings.getOptLevel();
					stmt.execute(optLevelQuery);
					}
				catch(SQLException se) {
					ServerMessageUtil.SPrintToConsole("Database : Error setting the Optimization Level of presto : "+se);
					}
			}
			return true;
		}*/
		return false;
	}
	
	public Histogram getHistogram(String tabName, String schema, String attribName) throws PicassoException
	{
		return new PrestoHistogram(this, tabName, schema, attribName);
	}
	
//	Presto server doesn't have plantables
	public void emptyPlanTable(){ }
	public void removeFromPlanTable(int qno){ }
	
	public boolean checkPlanTable()
	{
		return true;
	}

	@Override public boolean connect() throws PicassoException {
		return super.connect();
	}

	protected void createPicassoColumns(Statement stmt) throws SQLException
	{
		stmt.executeUpdate("create view "+settings.getSchema()+".picasso_columns as SELECT COLUMN_NAME, TABLE_NAME, TABLE_SCHEMA AS owner" +
		" FROM  INFORMATION_SCHEMA.COLUMNS");
	}
	
	protected void createRangeResMap(Statement stmt) throws SQLException //-ma
	{
		stmt.executeUpdate("create table "+settings.getSchema()+".PicassoRangeResMap (DIMNUM int , RESOLUTION int, QTID int) WITH (partitioned_by = ARRAY['QTID'])");
	}
	
	protected void createQTIDMap(Statement stmt) throws SQLException
	{
		stmt.executeUpdate("create table "+settings.getSchema()+".PicassoQTIDMap ( QTID int, QTEMPLATE varchar(64000), " +
				"RESOLUTION integer, DIMENSION integer, EXECTYPE varchar(" + PicassoConstants.SMALL_COLUMN + "), DISTRIBUTION varchar(" + PicassoConstants.SMALL_COLUMN + "), " +
		"OPTLEVEL varchar(" + PicassoConstants.QTNAME_LENGTH + "), PLANDIFFLEVEL varchar(" + PicassoConstants.SMALL_COLUMN + "), GENTIME bigint, GENDURATION bigint, QTNAME varchar(" + PicassoConstants.MEDIUM_COLUMN + ")) WITH (partitioned_by = ARRAY['QTNAME'])");
	}
	
	protected void createPlanTree(Statement stmt) throws SQLException
	{
		stmt.executeUpdate("create table "+settings.getSchema()+".PicassoPlanTree (PLANNO int , ID int , PARENTID int , "+
				"NAME varchar(" + PicassoConstants.MEDIUM_COLUMN + "), COST real, CARD real,  QTID int) WITH (partitioned_by = ARRAY['QTID'])");
	}
	
	protected void createPlanTreeArgs(Statement stmt) throws SQLException 
	{
		stmt.executeUpdate(
				"create table " + settings.getSchema() + ".PicassoPlanTreeArgs (PLANNO int , ID int , "
						+ "ARGNAME varchar(" + PicassoConstants.SMALL_COLUMN + ") , ARGVALUE varchar("
						+ PicassoConstants.SMALL_COLUMN + "), QTID int) WITH (partitioned_by = ARRAY['QTID'])");
	}
	
	protected void createXMLPlan(Statement stmt) throws SQLException
	{
	}
	
	protected void createPlanStore(Statement stmt) throws SQLException
	{
		stmt.executeUpdate("create table "+settings.getSchema()+".PicassoPlanStore (QID int , PLANNO int, COST double precision, CARD double precision, " +
		"RUNCOST double precision, RUNCARD double precision, QTID int) WITH (partitioned_by = ARRAY['QTID'])");
	}
	
	protected void createSelectivityMap(Statement stmt) throws SQLException
	{
		stmt.executeUpdate("create table "+settings.getSchema()+".PicassoSelectivityMap ( QID int , DIMENSION int , " +
		"SID int, QTID int) WITH (partitioned_by = ARRAY['QTID'])");
	}
	
	protected void createSelectivityLog(Statement stmt) throws SQLException
	{
		stmt.executeUpdate("create table "+settings.getSchema()+".PicassoSelectivityLog (DIMENSION int , SID int , " +
				"PICSEL double precision, PLANSEL double precision, PREDSEL double precision, DATASEL double precision, CONST varchar(" + PicassoConstants.SMALL_COLUMN + "), QTID int) WITH (partitioned_by = ARRAY['QTID'])");
	}
	
	protected void createApproxSpecs(Statement stmt) throws SQLException
	{
		stmt.executeUpdate("create table "+settings.getSchema()+".PicassoApproxMap (SAMPLESIZE real, SAMPLINGMODE int, AREAERROR real, IDENTITYERROR real,FPCMODE int,  QTID int) WITH (partitioned_by = ARRAY['QTID'])");
	}
	
	 protected void createPicassoHistogram(Statement stmt) throws SQLException
		{
		 try {
				stmt.executeUpdate("create table " +settings.getSchema()+ ".picassohistogram (SCHEMANAME VARCHAR (20) ," +
				"TABLENAME VARCHAR (64) , COLUMNNAME VARCHAR (20) ,ISHISTOGRAM VARCHAR (4))");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	 
	 
	 public void enterNode(int id, int pid, String name, double cost, double card, String arg){
		    
	    	try {
				Statement stmt2 = createStatement();
				stmt2.executeUpdate("INSERT INTO "+settings.getSchema()+".EXPLAINTABLE VALUES ("+id+","+pid+",'"+name.trim()+"',"+cost+","+card+",'"+arg.trim()+"')");
				stmt2.close();	
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 		
	    }
	
		double filter= 1; 
		int flag =1;

  public Plan getPlan(String query) throws PicassoException {
    //System.out.println("Query :"+query);

    String textualPlan;

    try{
      //System.out.println("Trying to Explain Query");
      Statement stmt = createStatement();
      ResultSet rs = stmt.executeQuery("EXPLAIN "+query);
//      while(rs.next())
//        textualPlan.add(rs.getString(1));
			rs.next();
			textualPlan = (rs.getString(1));
      rs.close();
      stmt.close();
      String[] splits = textualPlan.split("\\r?\\n");
      return getPlanFromText(splits);

//      if(textualPlan.size()<=0)
//        return null;
//                        /*ListIterator it = textualPlan.listIterator();
//                        System.out.println("Parsing Query Plan");
//                        while(it.hasNext())
//                                System.out.println((String)it.next());*/
    }catch(Exception e){
      e.printStackTrace();
      ServerMessageUtil.SPrintToConsole("Cannot get plan from presto: "+e);
      throw new PicassoException("Error getting plan: "+e);
    }
    //parseNode(plan,0,-1,textualPlan);
    //plan.show();


    /*CreateNode(plan, (String)textualPlan.remove(0), 0, -1);
    FindChilds(plan, 0, 1, textualPlan, 2);
    SwapSORTChilds(plan);
    return plan;*/
  }


  public static class NodeIdBuilder{
    Node node;
    int depth;

    public NodeIdBuilder(int depth, Node node) {
      this.node = node;
      this.depth = depth;
    }

    public Node getNode() {
      return node;
    }

    public int getDepth() {
      return depth;
    }
  }

  private static int findMyParentNodeId(int myIdx, NodeIdBuilder[] nodes) {
    int myDepth = nodes[myIdx].getDepth();
    int parentDepth = myDepth - 1;
    for (int i = myIdx -1; i >= 0; i--){
      if (nodes[i].getDepth() == parentDepth) {
        return nodes[i].getNode().getId();
      }
    }
    return -1;
  }

  public static Plan getPlanFromText(String[] lines) {
		Map<Integer, Integer> indentMap = new HashMap<Integer, Integer>();
  	int nodeCount = 0;
    Plan plan = new Plan();
    ArrayList<NodeIdBuilder> build = new ArrayList<>();
    int uniq = 0;
    Pattern re = Pattern.compile("(└─ |├─ )([a-zA-Z]*)");
    Boolean fillNode = false;
    for (int i = 0; i < lines.length; i++) {
			fillNode = false;
			String line = lines[i];
			Node node = new Node();
			Matcher m = re.matcher(line);
			int depth = -1;
			if (m.find()) {
				if (indentMap.get(line.indexOf(m.group(1))) == null) {
					indentMap.put(line.indexOf(m.group(1)), indentMap.get(line.indexOf(m.group(1)) - 3) + 1);
				}
				node.setName(m.group(2));
				depth = indentMap.get(line.indexOf(m.group(1)));
				build.add(new NodeIdBuilder(depth, node));
				if (depth == 0) {
					node.setParentId(0);
				} else {
					node.setParentId(
							findMyParentNodeId(build.size() - 1, build.toArray(new NodeIdBuilder[build.size()])));
				}
				node.setId(uniq++);
				fillNode = true;
				if (line.contains("[table = ")) {
					Node ntab = new Node();
					ntab.setId(-1);
					ntab.setParentId(node.getId());
					String word = line.substring(line.indexOf("[table = ") + "[table = ".length());
					Pattern tabp = Pattern.compile("([a-zA-Z0-9_]+)(:)([a-zA-Z0-9_]+)(:)([a-zA-Z0-9_]+)");
					Matcher m1 = tabp.matcher(word);
					if (m1.find()) {
						ntab.setName(m1.group(5));
					}
					plan.setNode(ntab, nodeCount++);
				}
			} else if (line.contains("Output")) {
				indentMap.put(line.indexOf("Output"), 0);
				node.setName("Output");
				node.setId(uniq++);
				depth = indentMap.get(line.indexOf("Output"));
				node.setParentId(-1);
				build.add(new NodeIdBuilder(depth, node));
				fillNode = true;
			} else {
				// ??
			}

			if (fillNode) {
				try {
					line = lines[i + 2];
					if (line.contains("Estimates:")) {
						int cost = 0, card = 0;
						//TODO: cost calculation can go wrong when multiple operators present, just need to take the last one ?
						String str = line.substring(line.indexOf("Estimates:"));
						String[] splitted = str.split("\\s+");
						for (int j = 0; j < splitted.length; j++) {
							if (splitted[j].contains("{rows:")) {
								card = new Integer(splitted[j + 1].replaceAll("\\D+", ""));
							} else if ("cpu:".equals(splitted[j])) {
								cost += new Integer(splitted[j + 1].replaceAll("\\D+", ""));
							} else if ("memory:".equals(splitted[j])) {
								cost += new Integer(splitted[j + 1].replaceAll("\\D+", ""));
							} else if ("network:".equals(splitted[j])) {
								cost += new Integer(splitted[j + 1].replaceAll("\\D+", ""));
							}
						}

						node.setCost(cost);
						node.setCard(card);
						plan.setNode(node, nodeCount++);
						i += 2;
					}
				} catch (Exception ex) {
					node.setCost(11);
					node.setCard(20);
					plan.setNode(node, nodeCount++);
					i += 2;
				}

				/*line = lines[i + 2];
				if(line.contains("Estimates:")) {
					int cost = 0, card = 0;
					String str = line.substring(line.indexOf("Estimates:"));
					String[] splitted = str.split("\\s+");
					for (int j = 0; j < splitted.length; j++) {
						if ("{rows:".equals(splitted[j])) {
							card = new Integer(splitted[j + 1].replaceAll("\\D+", ""));
						} else if ("cpu:".equals(splitted[j])) {
							cost += new Integer(splitted[j + 1].replaceAll("\\D+", ""));
						} else if ("memory:".equals(splitted[j])) {
							cost += new Integer(splitted[j + 1].replaceAll("\\D+", ""));
						} else if ("network:".equals(splitted[j])) {
							cost += new Integer(splitted[j + 1].replaceAll("\\D+", ""));
						}
					}

					node.setCost(cost);
					node.setCard(card);
					plan.setNode(node, nodeCount++);
					i += 2;
				}*/
			}
		}
    return plan;
  }

	public void processExplain(int tabCnt) {
		
		Plan plan1 = new Plan();
		int i=0,cnt=0,count;
		
		int id,pid;
		double cost,card;
		String name;
		String arg;		
		
		
		try {
			
			Statement stmt = createStatement ();
			ResultSet rset=stmt.executeQuery("select count(*) from "+settings.getSchema()+".EXPLAINTABLE");
			rset.next();
			count = rset.getInt(1);
			rset.close();
			stmt.close();
			
			count-=tabCnt;
			
			 stmt = createStatement ();
			 rset=stmt.executeQuery("select * from "+settings.getSchema()+".EXPLAINTABLE");
			
			while(rset.next()){
				
				id=rset.getInt(1);
				pid=rset.getInt(2);
				name=rset.getString(3);
				cost=rset.getDouble(4);
				card=rset.getDouble(5);
				arg = rset.getString(6);
				
				if(qno!=PicassoConstants.HIGH_QNO-1){
				if(id != -1)
					id=count - id;
				pid = count - pid;
				}
				else
					card=card*filter/100;
				Statement stmt2 = createStatement();
				stmt2.executeUpdate("INSERT INTO "+settings.getSchema()+".TEMP_EXPLAINTABLE VALUES ("+id+","+pid+",'"+name.trim()+"',"+cost+","+card+",'"+arg.trim()+"')");
				stmt2.close();	
			}
			
			rset.close();
			stmt.close();
			Statement stmt2 = createStatement(); 
    		stmt2.executeUpdate("delete from "+settings.getSchema()+".EXPLAINTABLE");
    		stmt2.close();
    		
    		stmt2 = createStatement(); 
    		ResultSet rset2=stmt2.executeQuery("select * from "+settings.getSchema()+".TEMP_EXPLAINTABLE order by ID");
    		while(rset2.next())
    		{
    			id=rset2.getInt(1);
				pid=rset2.getInt(2);
				name=rset2.getString(3);
				cost=rset2.getDouble(4);
				card=rset2.getDouble(5);
				arg = rset2.getString(6);
				Statement stmt3 = createStatement();
				stmt3.executeUpdate("INSERT INTO "+settings.getSchema()+".EXPLAINTABLE VALUES ("+id+","+pid+",'"+name.trim()+"',"+cost+","+card+",'"+arg.trim()+"')");
				stmt3.close();	
    		}
    		
    		rset2.close();
    		stmt2.close();
			
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		try {
			Statement stmt = createStatement ();
			ResultSet rset=stmt.executeQuery("select * from "+settings.getSchema()+".EXPLAINTABLE");
			
			while(rset.next()){
				
				id=rset.getInt(1);
				pid=rset.getInt(2);
				name=rset.getString(3);
				cost=rset.getDouble(4);
				card=rset.getDouble(5);
				arg = rset.getString(6);
				
				if(rset.getInt(1) == -1){
				Node temp_node = new Node();
					temp_node.setId(id);
					temp_node.setParentId(pid);
					temp_node.setName(name);
					temp_node.setCost(cost);
					temp_node.setCard(card);
					temp_node.addArgValue(arg);
					plan1.setNode(temp_node,i);
					i++;
				}
				else
				{
					Statement stmt2 = createStatement();
					stmt2.executeUpdate("INSERT INTO "+settings.getSchema()+".NEW_EXPLAINTABLE VALUES ("+id+","+pid+",'"+name.trim()+"',"+cost+","+card+",'"+arg.trim()+"')");
					stmt2.close();	
				}
			}
			rset.close();
			stmt.close();
			
		Node new_node = new Node();
		
			while(cnt<i){
				new_node = plan1.getNode(cnt);
				id = new_node.getId();
				pid = new_node.getParentId();
				name = new_node.getName();
				cost = new_node.getCost();
				card = new_node.getCard();
				arg = new_node.getArgValue().toString();
				
				Statement stmt2 = createStatement();
				stmt2.executeUpdate("INSERT INTO "+settings.getSchema()+".NEW_EXPLAINTABLE VALUES ("+id+","+pid+",'"+name.trim()+"',"+cost+","+card+",'"+arg.trim()+"')");
				stmt2.close();	
			cnt++;
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Plan generateTree(int tabCnt) {
		
		Plan plan = new Plan();
		int curNode = 0, cnt=0;
		try {
			processExplain(tabCnt);
			
			Statement stmt = createStatement ();
			ResultSet rset=stmt.executeQuery("select * from "+settings.getSchema()+".NEW_EXPLAINTABLE");
						
			while(rset.next()){
				Node node = new Node();
				if(rset.getInt(1)== -1)
					node.setId(-1);
				else
					node.setId(rset.getInt(1));
				
				node.setParentId(rset.getInt(2));
				node.setName(rset.getString(3).trim());
				node.setCost(rset.getDouble(4));
				node.setCard(rset.getDouble(5));
				node.addArgType("Argument: ");
				node.addArgValue(rset.getString(6).trim());
				plan.setNode(node, curNode);
				curNode++;		
			}
			rset.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return plan;
		
	}
	
public void createExplainTable(){
    	
    	try {
    		Statement stmt2 = createStatement(); 
			stmt2.executeUpdate("create table " +settings.getSchema()+ ".EXPLAINTABLE (id int ,pid  int , name varchar (50) , " +
					"cost double precision , cardinality double precision , arg varchar (50))");
			stmt2.close();
			

			stmt2 = createStatement(); 
			stmt2.executeUpdate("create table " +settings.getSchema()+ ".TEMP_EXPLAINTABLE (id int ,pid  int , name varchar (50) , " +
					"cost double precision , cardinality double precision , arg varchar (50))");
			stmt2.close();
			
			stmt2 = createStatement(); 
			stmt2.executeUpdate("create table " +settings.getSchema()+ ".NEW_EXPLAINTABLE (id int ,pid  int , name varchar (50) , " +
					"cost double precision , cardinality double precision , arg varchar (50))");
			stmt2.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
    }
	 public void deleteExplainTable(){
	    	
	    	try {
	    		Statement stmt2 = createStatement(); 
	    		stmt2.executeUpdate("drop table "+settings.getSchema()+".EXPLAINTABLE");
	    		stmt2.close();
	    		
	    		stmt2 = createStatement(); 
	    		stmt2.executeUpdate("drop table "+settings.getSchema()+".TEMP_EXPLAINTABLE");
	    		stmt2.close();
	    		
	    		stmt2 = createStatement(); 
	    		stmt2.executeUpdate("drop table "+settings.getSchema()+".NEW_EXPLAINTABLE");
	    		stmt2.close();
	    		}
	    	catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	    }
	
	public Plan getPlan(String query,int startQueryNumber) throws PicassoException
	{
		int tmp = qno;
		qno = startQueryNumber-1;
		flag=0;
		Plan plan = getPlan(query);
		flag=1;
		qno = tmp;
		return plan;
	}
	
	
	public void deletePicassoTables() throws PicassoException
	{
		try{
			Statement stmt = createStatement();
			stmt.executeUpdate("drop table "+getSchema()+".PicassoSelectivityLog");
			stmt.executeUpdate("drop table "+getSchema()+".PicassoSelectivityMap");
			stmt.executeUpdate("drop table "+getSchema()+".PicassoPlanTreeArgs");
			stmt.executeUpdate("drop table "+getSchema()+".PicassoPlanTree");
			stmt.executeUpdate("drop table "+getSchema()+".PicassoPlanStore");
			stmt.executeUpdate("drop table "+getSchema()+".PicassoRangeResMap"); //-ma
			stmt.executeUpdate("drop table "+getSchema()+".PicassoApproxMap");
			stmt.executeUpdate("drop table "+getSchema()+".PicassoQTIDMap");
			stmt.close();
		}catch(Exception e){
			e.printStackTrace();
			ServerMessageUtil.SPrintToConsole("Error Dropping Picasso Tables"+e);
			throw new PicassoException("Error Dropping Picasso Tables"+e);
		}
	}
}

