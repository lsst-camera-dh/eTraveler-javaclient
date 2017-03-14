package org.lsst.camera.etraveler.javaclient.getHarnessed;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.StringUtils;

//import org.lsst.camera.etraveler.backend.db.DbConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ParameterMetaData;
import java.sql.SQLException;

/*
   For now client must get a db connection and pass in
 */
public class GetHarnessedData {
  private Connection m_connect=null;
  private String m_hardwareType=null;
  private String m_travelerName=null;
  private String m_schemaName=null;
  private String m_model=null;
  private String m_expSN=null;
  private int m_run=0;
  private int m_oneRai;
  private int m_oneHid;
  private ImmutablePair<String, Object> m_filter=null;
  
  private String m_raiList=null;
  // Per run information (run number) keyed by root activity id
  private HashMap<Integer, String > m_raiMap=null;

  // Per component information (experimentSN) keyed by hardware id
  private HashMap<Integer, String> m_hMap = null;
  
  private HashMap<String, Object> m_results=null;

  private static final int DT_ABSENT = -2, DT_UNKNOWN = -1,
    DT_FLOAT = 0, DT_INT = 1, DT_STRING = 2;

  /**
     Argument should be string representation of either a valid
     positive integer or valid positive integer followed by one
     non-numeric character, such as "1234D"
     Return integer equivalent, ignoring final non-numeric character
     if there is one.
   */
  private static int formRunInt(String st) throws GetHarnessedException {
    int theInt;
    try {
      theInt = Integer.parseInt(st);
      } catch (NumberFormatException e) {
      try {
        theInt = Integer.parseInt(st.substring(0, st.length() -1));
      }  catch (NumberFormatException e2) {
        throw new GetHarnessedException("Supplied run value " + st +
                                        " is not valid");
      }
      if (theInt < 1) {
        throw new GetHarnessedException("Supplied run value " + st +
                                        " is not valid");
      }
    }
    return theInt;
  }
  private static boolean
    storeInInstances(ArrayList< HashMap<String, Object> >instances,
                     ResultSet rs, int datatype)
    throws SQLException, GetHarnessedException {
    int schemaInstance = rs.getInt("ressI");
    HashMap<String, Object> myInstance=null;
    boolean gotRow = true;

    for (HashMap<String, Object> iMap : instances ) {
      if ((int) iMap.get("schemaInstance") == schemaInstance) {
        myInstance = iMap;
        if ((Integer) myInstance.get("activityId") != rs.getInt("aid")) {
          if ((Integer) myInstance.get("processId") != rs.getInt("pid")) {
            // Need a new instance after all
            myInstance = null;
          }  else { // skip past everything with this activityId
            int thisAid= rs.getInt("aid");
            while (thisAid == rs.getInt("aid") ) {
              gotRow = rs.relative(1);
              if (!gotRow) return gotRow;
            }
            return gotRow;
          }
        }
        break;
      }
    }
    if (myInstance == null) {
      myInstance = new HashMap<String, Object>();
      myInstance.put("schemaInstance", schemaInstance);
      myInstance.put("activityId", rs.getInt("aid"));
      myInstance.put("processId", rs.getInt("pid"));
      myInstance.put("processName", rs.getString("pname"));
      instances.add(myInstance);
    }
    HashMap<String, Object> instance0 =
      (HashMap<String, Object>) instances.get(0);
    switch (datatype) {
    case DT_FLOAT:
      myInstance.put(rs.getString("resname"), rs.getDouble("resvalue"));
      instance0.put(rs.getString("resname"), "float");
      break;
    case DT_INT:
      myInstance.put(rs.getString("resname"), rs.getInt("resvalue"));
      instance0.put(rs.getString("resname"), "int");
      break;
    case DT_STRING:
      myInstance.put(rs.getString("resname"), rs.getString("resvalue"));
      instance0.put(rs.getString("resname"), "string");
      break;
    default:
      throw new GetHarnessedException("Unkown datatype");
    }
    return rs.relative(1);
  }
    
  private static int
    pruneInstances(Pair<String, Object> filter,
                   ArrayList< HashMap<String, Object> > instances,
                   int dtype) throws GetHarnessedException {
      if (dtype == DT_ABSENT) return dtype;

    String key = filter.getLeft();
    Object val = filter.getRight();

    int valType = dtype;
    if (valType == DT_UNKNOWN) {
      if (!instances.get(0).containsKey(key)) return DT_ABSENT;
      String t=(String) instances.get(0).get(key);
      if (t.equals("float") ) {
        valType=DT_FLOAT;
      } else {
        if (t.equals("int")) {
          valType=DT_INT;
        } else {
          if (t.equals("string")) {
            valType=DT_STRING;
          }  else {
            throw new GetHarnessedException("pruneInstances: Unrecognized data type");
          }
        }
      }
    }
    for (int i=(instances.size() - 1); i > 0; i--) {
      if (!instances.get(i).get(key).equals(val) ) {
        instances.remove(i);
      }
    }
    return valType;
  }
    
  public GetHarnessedData(Connection conn) {
    m_connect=conn;
  }
  
  public void setConnection(Connection conn) {
    m_connect = conn;
  }
  /**
     travelerName must be non-null
     hardwareType must be non-null
     schemaName must be non-null to start; might loosen this requirements
     model, experimentSN are used for filtering if non-null
   */
  public Map<String, Object>
    getResultsJH(String travelerName, String hardwareType, String schemaName,
                 String model, String experimentSN,
                 Pair<String, Object> filter)
    throws GetHarnessedException, SQLException {
    if (m_connect == null)
      throw new GetHarnessedException("Set connection before attempting to fetch data");
    checkNull(travelerName, "travelerName argument must be non-null");
    checkNull(hardwareType, "hardwareType argument must be non-null");
    checkNull(schemaName, "schemaName argument must be non-null");

    clearCache();
    m_travelerName = travelerName;
    m_hardwareType = hardwareType;
    m_schemaName=schemaName;
    m_model = model;
    m_expSN = experimentSN;
    if (filter != null) {
      m_filter = new
        ImmutablePair<String, Object> (filter.getLeft(), filter.getRight());
    }
    getRaiMap();

    // There are 6 replacements to be made.  All of them are results table
    // name (e.g. "FloatResultHarnessed")
    String sqlString=
      "select ?.name as resname,?.value as resvalue,?.schemaInstance as ressI,A.id as aid,A.rootActivityId as raid, A.hardwareId as hid,A.processId as pid,Process.name as pname,ASH.activityStatusId as actStatus from  ? join Activity A on ?.activityId=A.id join ActivityStatusHistory ASH on A.id=ASH.activityId join ActivityFinalStatus on ActivityFinalStatus.id = ASH.activityStatusId join Process on Process.id=A.processId where ActivityFinalStatus.name='success' and ?.schemaName='" + m_schemaName;
    sqlString += "' and A.rootActivityId in " + m_raiList + " order by A.hardwareId asc, A.rootActivityId desc, A.processId asc, A.id desc, ressI asc, resname";

    
    m_results = new HashMap<String, Object>();
    executeGenQuery(sqlString, "FloatResultHarnessed", DT_FLOAT);
    executeGenQuery(sqlString, "IntResultHarnessed", DT_INT);
    executeGenQuery(sqlString, "StringResultHarnessed", DT_STRING);

    if (filter != null) prune(); 
    return m_results;
  }

  public Map<String, Object>
    getRunResults(String run, String schemaName, Pair<String, Object> filter)
    throws GetHarnessedException, SQLException {

    int runInt = GetHarnessedData.formRunInt(run);
    return getRunResults(runInt, schemaName, filter);
  }


  public Map<String, Object>
    getRunResults(int runInt, String schemaName, Pair<String, Object> filter)
    throws GetHarnessedException, SQLException {
        if (m_connect == null)
      throw new GetHarnessedException("Set connection before attempting to fetch data");
    checkNull(schemaName, "schemaName argument must be non-null");
    clearCache();

    m_schemaName = schemaName;
    m_run = runInt;

    return getRunResults(filter);
  }

  public Map<String, Object>
    getRunResults(String run, Pair<String, Object> filter)
    throws GetHarnessedException, SQLException {
    int runInt = GetHarnessedData.formRunInt(run);
    return getRunResults(runInt, filter);
  }

  
  public Map<String, Object>
    getRunResults(int runInt, Pair<String, Object> filter)
    throws GetHarnessedException, SQLException {
    if (m_connect == null)
      throw new GetHarnessedException("Set connection before attempting to fetch data");
    clearCache();

    m_run = runInt;

    return getRunResults(filter);
  }

  /**
     Does most of the work after public routines handle other arguments
     and appropriately set m_schemaName and m_run
   */
  private Map<String, Object>
    getRunResults(Pair<String, Object> filter)
    throws GetHarnessedException, SQLException {

    if (filter != null) {
      m_filter = new
        ImmutablePair<String, Object> (filter.getLeft(), filter.getRight());
    }

    getRunParameters();
    m_results = new HashMap<String, Object>();
    m_results.put("run", m_run);
    m_results.put("experimentSN", m_expSN);
    m_results.put("hid", m_oneHid);
    HashMap<String, PerSchema> schemaMap = new HashMap<String, PerSchema>();
    m_results.put("schemas", schemaMap);

    String sql =
     "select ?.schemaName as schname,?.name as resname,?.value as resvalue,?.schemaInstance as ressI,A.id as aid,A.processId as pid,Process.name as pname,ASH.activityStatusId as actStatus from ? join Activity A on ?.activityId=A.id join ActivityStatusHistory ASH on A.id=ASH.activityId join ActivityFinalStatus on ActivityFinalStatus.id=ASH.activityStatusId join Process on Process.id=A.processId where ";
    if (m_schemaName != null) {
      sql += "?.schemaName='" + m_schemaName +"' and ";
    }
    
    sql += " A.rootActivityId='" + m_oneRai + "' and ActivityFinalStatus.name='success' order by schname,A.processId asc,A.id desc, ressI asc, resname";

    executeGenRunQuery(sql, "FloatResultHarnessed", DT_FLOAT);
    executeGenRunQuery(sql, "IntResultHarnessed", DT_INT);
    executeGenRunQuery(sql, "StringResultHarnessed", DT_STRING);

    if (filter != null) {
      for (String schName : schemaMap.keySet()) {
        PerSchema per = schemaMap.get(schName);
        int dtype = DT_UNKNOWN;
        per.prune(filter, dtype);
      }
    }
    return m_results;
  }    
  
  /* This subquery is used to find possibly interesting traveler root ids */
  private String hidSubquery() {
    String subq = "select H2.id as hid2 from Hardware H2 join HardwareType HT on H2.hardwareTypeId=HT.id where HT.name='" + m_hardwareType + "'";
    if (m_expSN != null) {
      subq += " and H2.lsstId='" + m_expSN + "'";
    } else if (m_model != null) {
      subq += " and H2.model='" + m_model + "'";
    }
    return subq;
  }

  /**
     Find per-run parameters when we already have a run
  */
  private void getRunParameters() throws SQLException,GetHarnessedException {
    String sql= "select RunNumber.rootActivityId as rai, Activity.hardwareId as hid, Hardware.lsstId as expSN, HardwareType.name as hname,Process.name as pname from RunNumber join Activity on RunNumber.rootActivityId=Activity.id join Hardware on Activity.hardwareId=Hardware.id join HardwareType on HardwareType.id=Hardware.hardwareTypeId join Process on Process.id=Activity.processId where runInt='" + m_run + "'";
    ResultSet rs = m_connect.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE).executeQuery();
    if (!rs.first()) {
      throw new GetHarnessedException("No data found for run " + m_run);
    }
    m_oneRai = rs.getInt("rai");
    m_expSN = rs.getString("expSN");
    m_oneHid = rs.getInt("hid");
    m_hardwareType = rs.getString("hname");
    m_travelerName = rs.getString("pname");

    return;
  }
  /**
     Initialize per-run map and string rep. of runs
   */
  private void getRaiMap() throws SQLException {
    String hidSub = hidSubquery();

    String raiQuery = "select A.id as Aid, H.id as Hid, H.lsstId as expSN, runNumber from Hardware H join Activity A on H.id=A.hardwareId join Process P on A.processId=P.id join RunNumber on A.rootActivityId=RunNumber.rootActivityId where H.id in (" + hidSub + ") and A.id=A.rootActivityId and P.name='" + m_travelerName + "' order by H.id asc, A.id desc";

    PreparedStatement stmt =
      m_connect.prepareStatement(raiQuery, ResultSet.TYPE_SCROLL_INSENSITIVE);

    ResultSet rs = stmt.executeQuery();
    boolean gotRow  = rs.first();

    boolean first = true;
    if (gotRow) {
      m_raiMap = new HashMap<Integer, String>();
      m_hMap = new HashMap<Integer, String>();
      m_raiList= "(";
    }
    while (gotRow)  {
      m_raiMap.put((Integer)rs.getInt("Aid"), rs.getString("runNumber"));
      m_hMap.put((Integer)rs.getInt("Hid"), rs.getString("expSN"));
      if (!first) m_raiList += ",";
      else first = false;
      m_raiList += "'" + rs.getString("Aid") + "'";
      gotRow = rs.relative(1);
    }
    m_raiList += ")";

    stmt.close();
  }

  private void executeGenQuery(String sql, String tableName, int datatype)
    throws SQLException, GetHarnessedException {
    String sqlString = sql.replace("?", tableName);

    PreparedStatement genQuery =
      m_connect.prepareStatement(sqlString, ResultSet.TYPE_SCROLL_INSENSITIVE);

    ResultSet rs = genQuery.executeQuery();

    boolean gotRow = rs.first();
    if (!gotRow) {
      genQuery.close();
      return;
    }
    if (m_schemaName != null) {
      while (gotRow) {
        String expSN = m_hMap.get(rs.getInt("hid"));
        /* New stuff starts here */
        HashMap<String, Object> expMap = new HashMap<String, Object>();
        m_results.put(expSN, expMap);
        expMap.put("hid", rs.getInt("hid"));
        expMap.put("raid", rs.getInt("raid"));
        // Also fetch run number and put it in here
        HashMap<String, PerSchema> schemas =
          new HashMap<String, PerSchema>();
        
        gotRow = storeData(expSN, rs, datatype);
      }
    } else {
      throw new GetHarnessedException("Missing schema name");
    }
    genQuery.close();
    
  }

  private void executeGenRunQuery(String sql, String tableName, int datatype)
    throws SQLException, GetHarnessedException {
    String sqlString = sql.replace("?", tableName);

    PreparedStatement genQuery =
      m_connect.prepareStatement(sqlString, ResultSet.TYPE_SCROLL_INSENSITIVE);

    ResultSet rs = genQuery.executeQuery();

    boolean gotRow = rs.first();
    if (!gotRow) {
      genQuery.close();
      return;
    }
    storeRunAll(m_results.get("schemas"), rs, datatype, 0);
    genQuery.close();
  }
  /**
     Store a single key-value pair, creating and initializing 
     containing maps as needed.  Advance to next interesting row
     Return false data are exhausted
  */
  private boolean storeData(String expSN, 
                         ResultSet rs, int datatype)
    throws GetHarnessedException, SQLException {
    HashMap<String, Object> expMap=null;
    if (! m_results.containsKey(expSN) ) {
      // Add new key for expSN with  new map as value
      expMap = new HashMap<String, Object>();
      m_results.put(expSN, expMap);
      expMap.put("hid", rs.getInt("hid"));
      // expMap.put("aid", rs.getInt("aid"));
      expMap.put("raid", rs.getInt("raid"));
      ArrayList <HashMap<String, Object> > instances = new
        ArrayList< HashMap<String, Object> >();
      expMap.put("instances", instances);
      instances.add(new HashMap<String, Object>());
      instances.get(0).put("schemaInstance", (Integer) 0);
    } else {
      expMap= (HashMap<String, Object>) m_results.get(expSN);
    }
    boolean gotRow = true;

    if ((Integer) expMap.get("raid")  !=  rs.getInt("raid"))   {
      // Skip past all the rest of the data with this hid
      //System.out.println("raid for this component is " + expMap.get("raid"));
      //System.out.println("Skipping data from older run " + rs.getString("raid"));
      while (rs.getInt("hid") == (Integer) expMap.get("hid") ) {
        gotRow = rs.relative(1);
        if (!gotRow) return gotRow;
      }
      return gotRow;
    }

    ArrayList <HashMap<String, Object> > instances =
      (ArrayList <HashMap <String, Object> >) expMap.get("instances");

    return GetHarnessedData.storeInInstances(instances, rs, datatype);
  }

  /**
     Apply filter, discarding unwanted entries.  This implementation
     assumes all data comes from the same schema
  */
  private void prune() throws GetHarnessedException {
    String key = m_filter.getLeft();
    Object val = m_filter.getRight();
    boolean first = true;
    int  valType = DT_UNKNOWN;

    for (String expSN : m_results.keySet() ) {
      HashMap<String, Object> expMap =
        (HashMap<String, Object>) m_results.get(expSN);
      ArrayList< HashMap<String, Object> > iList =
        (ArrayList< HashMap<String, Object> >) expMap.get("instances");
      
      valType = GetHarnessedData.pruneInstances(m_filter, iList, valType);
      if (valType == DT_UNKNOWN) return;
    }
  }

  /**  
       Store everything for a run from one of the *ResultHarnessed tables 
       Upon entry the result set is pointing at the first row, known not to be null
       Return false if no more data; true if there is another run 
  */
  private boolean storeRunAll(Object schemaMapsObject, ResultSet rs, int datatype,
                           int rai) throws SQLException, GetHarnessedException {
    HashMap<String, PerSchema> schemaMaps =
      (HashMap<String, PerSchema>) schemaMapsObject;

    boolean gotRow = true;
    String schname = "";
    String pname = ""; 
    PerSchema ourSchemaMap = null;

    PerStep ourInstanceList = null;

    while (gotRow) {
      if (!(schname.equals(rs.getString("schname")))) {
        schname = rs.getString("schname");
        ourSchemaMap = getOurSchemaMap(schemaMaps, schname);
        pname = "";
      }
      if (!pname.equals(rs.getString("pname"))) {
        pname = rs.getString("pname");
        //ourInstanceList = getOurList(ourSchemaMap, pname);
        ourInstanceList = ourSchemaMap.findOrAddStep(pname);
      }
      gotRow = storeOne(rs, ourInstanceList.getArrayList(), datatype);
      if (rai > 0) {
        if (!gotRow) return gotRow;
        // If rai has changed, we're done with this component
        if (rs.getInt("rai") != rai) return gotRow; 
      }
    }
    return gotRow;
  }

  PerSchema getOurSchemaMap(HashMap<String, PerSchema> maps, String key)  {
    if (maps.containsKey(key)) return maps.get(key);
    PerSchema newMap = new PerSchema();
    maps.put(key, newMap);
    return newMap;
  }
  
  /* Store a row and advance cursor.  If schema name and pid match but aid does not, skip over rows
     until we've exhausted rows with that aid */
  private boolean storeOne(ResultSet rs, ArrayList<HashMap <String, Object> > instances, int datatype) 
    throws SQLException, GetHarnessedException {
    int schemaInstance = rs.getInt("ressI");
    HashMap<String, Object> myInstance=null;
    boolean gotRow = true;
    for (HashMap<String, Object> iMap : instances ) {
      if ((int) iMap.get("schemaInstance") == schemaInstance) {
        myInstance = iMap;
        if ((Integer) myInstance.get("activityId") != rs.getInt("aid")) {
          int thisAid = rs.getInt("aid");
          while (thisAid == rs.getInt("aid")) {
            gotRow = rs.relative(1);
            if (!gotRow) return gotRow;
          }
          return gotRow;
        }
        break;
      }
    }
    if (myInstance == null) {
      myInstance = new HashMap<String, Object>();
      myInstance.put("schemaInstance", schemaInstance);
      myInstance.put("activityId", rs.getInt("aid"));
      instances.add(myInstance);
    }
    HashMap<String, Object> instance0 =
      (HashMap<String, Object>) instances.get(0);
    switch (datatype) {
    case DT_FLOAT:
      myInstance.put(rs.getString("resname"), rs.getDouble("resvalue"));
      instance0.put(rs.getString("resname"), "float");
      break;
    case DT_INT:
      myInstance.put(rs.getString("resname"), rs.getInt("resvalue"));
      instance0.put(rs.getString("resname"), "int");
      break;
    case DT_STRING:
      myInstance.put(rs.getString("resname"), rs.getString("resvalue"));
      instance0.put(rs.getString("resname"), "string");
      break;
    default:
      throw new GetHarnessedException("Unkown datatype");
    }
    return rs.relative(1);
  }
      
  private void checkNull(String val, String msg) throws GetHarnessedException {
    if (val == null) throw new GetHarnessedException(msg);
  }

  /* Clear all local data except for connection */
  private void clearCache() {
    m_hardwareType=null;
    m_travelerName=null;
    m_schemaName=null;
    m_model=null;
    m_expSN=null;
    m_filter=null;
    m_raiList=null;
    m_raiMap=null;
    m_hMap=null;
    m_results=null;
    m_run=0;
    m_oneRai=0;
    m_oneHid=0;
  }

}
