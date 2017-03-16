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
  private String m_stepName=null;
  private ImmutablePair<String, Object> m_filter=null;
  
  private String m_raiList=null;
  // Per run information (run number) keyed by root activity id
  private HashMap<Integer, String > m_raiMap=null;

  // Per component information (experimentSN) keyed by hardware id
  private HashMap<Integer, String> m_hMap = null;
  
  private HashMap<String, Object> m_results=null;

  private HashMap<String, ArrayList<String> > m_fileResults=null;

  private static final int DT_ABSENT = -2, DT_UNKNOWN = -1,
    DT_FLOAT = 0, DT_INT = 1, DT_STRING = 2;

  // Assumes Activity has alias A
  private static final String activityStatusJoins="join ActivityStatusHistory ASH on A.id=ASH.activityId join ActivityFinalStatus on ActivityFinalStatus.id=ASH.activityStatusId";
  private static final String activityStatusCondition="ActivityFinalStatus.name='success'";
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
     Return data is map of maps (one for each component)
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
    if (!getRaiMap()) {
      throw new GetHarnessedNoDataException("No data found");
    }

    // There are 6 replacements to be made.  All of them are results table
    // name (e.g. "FloatResultHarnessed")
    String sqlString=
      "select ?.schemaName as schname,?.name as resname,?.value as resvalue,?.schemaInstance as ressI,A.id as aid,A.rootActivityId as raid, A.hardwareId as hid,A.processId as pid,Process.name as pname,ASH.activityStatusId as actStatus from  ? join Activity A on ?.activityId=A.id join ActivityStatusHistory ASH on A.id=ASH.activityId join ActivityFinalStatus on ActivityFinalStatus.id = ASH.activityStatusId join Process on Process.id=A.processId where ActivityFinalStatus.name='success' and ?.schemaName='" + m_schemaName;
    sqlString += "' and A.rootActivityId in " + m_raiList + " order by A.hardwareId asc, A.rootActivityId desc, A.processId asc, schname,A.id desc, ressI asc, resname";

    m_results = new HashMap<String, Object>();
    executeGenQuery(sqlString, "FloatResultHarnessed", DT_FLOAT);
    executeGenQuery(sqlString, "IntResultHarnessed", DT_INT);
    executeGenQuery(sqlString, "StringResultHarnessed", DT_STRING);

    if (filter != null)  {
      for (Object expObject : m_results.values()) {
        HashMap<String, Object> expMap = (HashMap<String, Object>) expObject;
        HashMap<String, PerStep> steps =
          (HashMap<String, PerStep> ) expMap.get("steps");
        for (PerStep per : steps.values()) {
          int dtype = DT_UNKNOWN;
          per.prune(filter, dtype);
        }
      }
    }
      //prune(); 
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
     For the given run return lists of virtual paths for all files registered in 
     Data Catalog, sorted by process step
   */
  public Map<String, ArrayList<String> > getFilepaths(String run, String stepName)
    throws GetHarnessedException, SQLException {
    int runInt = GetHarnessedData.formRunInt(run);
    return getFilepaths(runInt, stepName);
  }

  public Map<String, ArrayList<String> > getFilepaths(int run, String stepName)
    throws GetHarnessedException, SQLException {
    clearCache();
    m_run = run;
    m_stepName = stepName;
    // Really m_results will be HashMap<String, ArrayList<String> >();
    m_fileResults = new HashMap<String, ArrayList<String> >();

    String sql =
      "select F.virtualPath as vp,P.name as pname,P.id as pid,A.id as aid from FilepathResultHarnessed F join Activity A on F.activityId=A.id "
      + activityStatusJoins +
      " join Process P on P.id=A.processId join RunNumber on A.rootActivityId=RunNumber.rootActivityId where RunNumber.runInt='"
      + m_run + "' and " + activityStatusCondition; 
    if (m_stepName != null) {
      sql += " and P.name='" + m_stepName  + "' ";
    }
    sql += " order by P.id,A.id desc,F.virtualPath";
    PreparedStatement stmt = m_connect.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE);
    ResultSet rs = stmt.executeQuery();
    boolean gotRow=rs.first();
    if (!gotRow) {
      throw new GetHarnessedNoDataException("No files generated by run " + m_run);
    }
    storePaths(rs);
    return m_fileResults;
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
    HashMap<String, PerStep> stepMap = new HashMap<String, PerStep>();
    m_results.put("steps", stepMap);

    String sql =
     "select ?.schemaName as schname,?.name as resname,?.value as resvalue,?.schemaInstance as ressI,A.id as aid,A.processId as pid,Process.name as pname,ASH.activityStatusId as actStatus from ? join Activity A on ?.activityId=A.id join ActivityStatusHistory ASH on A.id=ASH.activityId join ActivityFinalStatus on ActivityFinalStatus.id=ASH.activityStatusId join Process on Process.id=A.processId where ";
    if (m_schemaName != null) {
      sql += "?.schemaName='" + m_schemaName +"' and ";
    }
    
    sql += " A.rootActivityId='" + m_oneRai + "' and ActivityFinalStatus.name='success' order by A.processId asc,schname, A.id desc, ressI asc, resname";

    executeGenRunQuery(sql, "FloatResultHarnessed", DT_FLOAT);
    executeGenRunQuery(sql, "IntResultHarnessed", DT_INT);
    executeGenRunQuery(sql, "StringResultHarnessed", DT_STRING);

    if (filter != null) {
      for (String pname : stepMap.keySet()) {
        PerStep per = stepMap.get(pname);
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
  private boolean getRaiMap() throws SQLException {
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
    } else {
      stmt.close();
      return false;
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
    return true;
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
    HashMap<String, Object> expMap = null;
    HashMap<String, PerStep> steps;
    if (m_schemaName != null) {
      while (gotRow) {
        String expSN = m_hMap.get(rs.getInt("hid"));
        /* New stuff starts here */
        if (m_results.containsKey(expSN) ) {
          expMap = (HashMap<String, Object>) m_results.get(expSN);
          steps = (HashMap<String, PerStep>) expMap.get("steps");
        } else  {
          expMap = new HashMap<String, Object>();
          m_results.put(expSN, expMap);
          expMap.put("hid", rs.getInt("hid"));
          expMap.put("raid", rs.getInt("raid"));
          expMap.put("runNumber", m_raiMap.get(rs.getInt("raid")));

          steps = new HashMap<String, PerStep>();
          expMap.put("steps", steps);
        }
        gotRow = storeRunAll(steps, rs, datatype, rs.getInt("hid"));
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
    storeRunAll(m_results.get("steps"), rs, datatype, 0);
    genQuery.close();
  }

  /**  
       Store everything for a run from one of the *ResultHarnessed tables 
       Upon entry the result set is pointing at the first row, known not to be null
       Return false if no more data; true if there is another run 
  */
  private boolean storeRunAll(Object stepMapsObject, ResultSet rs, int datatype,
                           int hid) throws SQLException, GetHarnessedException {
    HashMap<String, PerStep> stepMaps =
      (HashMap<String, PerStep>) stepMapsObject;

    boolean gotRow = true;
    String pname = ""; 
    String schname = "";
    PerStep ourStepMap = null;

    PerSchema ourInstanceList = null;

    int raid = 0;
    if (hid > 0) { // could be more than one run
      raid = rs.getInt("raid");
    }
    while (gotRow) {
      if (raid > 0) {
        if (rs.getInt("raid") != raid) {
          while (rs.getInt("hid") == hid) {
            gotRow = rs.relative(1);
            if (!gotRow) return gotRow;
          }
          return gotRow;
        }
      }
      if (!(pname.equals(rs.getString("pname")))) {
        pname = rs.getString("pname");
        ourStepMap = getOurStepMap(stepMaps, pname);
        schname = "";
      }
      if (!schname.equals(rs.getString("schname"))) {
        schname = rs.getString("schname");
        ourInstanceList = ourStepMap.findOrAddSchema(schname);
      }
      gotRow = storeOne(rs, ourInstanceList.getArrayList(), datatype);
      if (hid > 0) {
        if (!gotRow) return gotRow;
        // If hid has changed, we're done with this component
        if (rs.getInt("hid") != hid) return gotRow; 
      }
    }
    return gotRow;
  }

  PerStep getOurStepMap(HashMap<String, PerStep> maps, String key)  {
    if (maps.containsKey(key)) return maps.get(key);
    PerStep newMap = new PerStep();
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

  /**
     Do the work of storing returned data.  We start with cursor pointing to a good row
     For each process step, save data for most recent successful activity assoc. with that step
   */
  private void storePaths(ResultSet rs) throws SQLException {
    boolean gotRow=true;
    int  pid = 0;
    int  aid = 0;
    ArrayList<String> fileList=null;

    while (gotRow) {
      if (pid != rs.getInt("pid")) { // make a new one
        pid = rs.getInt("pid");
        fileList = new ArrayList<String>();
        m_fileResults.put(rs.getString("pname"), fileList);
        aid = rs.getInt("aid");
      }
      while ((aid != rs.getInt("aid")) && (pid == rs.getInt("pid")))  {   // skip past
        gotRow = rs.relative(1);
        if (!gotRow) return;
      }
      if (pid == rs.getInt("pid")) {
        fileList.add(rs.getString("vp"));
        gotRow = rs.relative(1);
      }   // otherwise this file belongs to a new step
    } 
    return;
  }
      
  private void checkNull(String val, String msg) throws GetHarnessedException {
    if (val == null) throw new GetHarnessedException(msg);
  }

  /* Clear all local data except for connection */
  private void clearCache() {
    m_hardwareType=null;
    m_travelerName=null;
    m_stepName=null;
    m_schemaName=null;
    m_model=null;
    m_expSN=null;
    m_filter=null;
    m_raiList=null;
    m_raiMap=null;
    m_hMap=null;
    m_results=null;
    m_fileResults=null;
    m_run=0;
    m_oneRai=0;
    m_oneHid=0;
  }

}
