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
import java.util.ArrayList;


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
  private ImmutablePair<String, Object> m_filter=null;
  
  private String m_raiList=null;
  // Per run information (run number) keyed by root activity id
  private HashMap<Integer, String > m_raiMap=null;

  // Per component information (experimentSN) keyed by hardware id
  private HashMap<Integer, String> m_hMap = null;
  
  // private PreparedStatement m_genQuery=null;
  private HashMap<String, Object> m_results=null;

  private static final int DT_FLOAT = 0, DT_INT = 1, DT_STRING = 2;
    
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
      "select ?.name as resname,?.value as resvalue,?.schemaInstance as ressI,A.id as aid,A.rootActivityId as raid, A.hardwareId as hid,ASH.activityStatusId as actStatus from  ? join Activity A on ?.activityId=A.id join ActivityStatusHistory ASH on A.id=ASH.activityId join ActivityFinalStatus on ActivityFinalStatus.id = ASH.activityStatusId where ActivityFinalStatus.name='success' and ?.schemaName='" + m_schemaName;
    sqlString += "' and A.rootActivityId in " + m_raiList + " order by A.hardwareId asc, A.rootActivityId desc, ressI asc, resname";

    
    m_results = new HashMap<String, Object>();
    executeGenQuery(sqlString, "FloatResultHarnessed", DT_FLOAT);
    executeGenQuery(sqlString, "IntResultHarnessed", DT_INT);
    executeGenQuery(sqlString, "StringResultHarnessed", DT_STRING);

    if (filter != null) prune(); 
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
     Initialize per-run map and string rep. of runs
   */
  private void getRaiMap() throws SQLException {
    String hidSub = hidSubquery();

    // Would use Prepared Statement if this could be reused
    String raiQuery = "select A.id as Aid, H.id as Hid, H.lsstId as expSN, runNumber from Hardware H join Activity A on H.id=A.hardwareId join Process P on A.processId=P.id join RunNumber on A.rootActivityId=RunNumber.rootActivityId where H.id in (" + hidSub + ") and A.id=A.rootActivityId and P.name='" + m_travelerName + "' order by H.id asc, A.id desc";

    PreparedStatement stmt =
      m_connect.prepareStatement(raiQuery, ResultSet.TYPE_SCROLL_INSENSITIVE);

    ResultSet rs = stmt.executeQuery();
    boolean gotRow  = rs.first();
    //if (gotRow)  m_raiMap = new HashMap<Integer, HashMap<String, String> >();
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
    /*
    int nSub = m_genQuery.getParameterMetaData().getParameterCount();
    for (int i=1; i<=nSub; i++) {
      m_genQuery.setString(i, tableName);
    }
    */
    ResultSet rs = genQuery.executeQuery();

    boolean gotRow = rs.first();
    while (gotRow) {
      String expSN = m_hMap.get(rs.getInt("hid"));
      storeData(expSN, rs, datatype);
      gotRow = rs.relative(1);
    }
    genQuery.close();
    
  }

  /**
     Store a single key-value pair, creating and initializing 
     containing maps as needed
  */
  private void storeData(String expSN, 
                         ResultSet rs, int datatype)
    throws GetHarnessedException, SQLException {
    HashMap<String, Object> expMap=null;
    if (! m_results.containsKey(expSN) ) {
      // Add new key for expSN with  new map as value
      expMap = new HashMap<String, Object>();
      m_results.put(expSN, expMap);
      expMap.put("hid", rs.getInt("hid"));
      expMap.put("aid", rs.getInt("aid"));
      expMap.put("raid", rs.getInt("raid"));
      ArrayList <HashMap<String, Object> > instances = new
        ArrayList< HashMap<String, Object> >();
      expMap.put("instances", instances);
      instances.add(new HashMap<String, Object>());
      instances.get(0).put("instanceNumber", (Integer) 0);
    } else {
      expMap= (HashMap<String, Object>) m_results.get(expSN);
    }
    int instanceNumber = rs.getInt("ressI");
    HashMap<String, Object> myInstance=null;
    ArrayList <HashMap<String, Object> > instances =
      (ArrayList <HashMap <String, Object> >) expMap.get("instances");
    for (HashMap<String, Object> iMap : instances ) {
      if ((int) iMap.get("instanceNumber") == instanceNumber) {
        myInstance = iMap;
        break;
      }
    }
    if (myInstance == null) {
      myInstance = new HashMap<String, Object>();
      myInstance.put("instanceNumber", instanceNumber);
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
    return;

  }

  /**
     Apply filter, discarding unwanted entries.  This implementation
     assumes all data comes from the same schema
  */
  private void prune() throws GetHarnessedException {
    String key = m_filter.getLeft();
    Object val = m_filter.getRight();
    boolean first = true;
    int  valType = -1;
    for (String expSN : m_results.keySet() ) {
      HashMap<String, Object> expMap =
        (HashMap<String, Object>) m_results.get(expSN);
      ArrayList< HashMap<String, Object> > iList =
        (ArrayList< HashMap<String, Object> >) expMap.get("instances");
      if (first) {
        if (!iList.get(0).containsKey(key)) return;

        /* Maybe we don't need this */
        String t=(String) iList.get(0).get(key);
        if (t.equals("float") ) {
          valType=DT_FLOAT;
        } else {
          if (t.equals("int")) {
            valType=DT_INT;
          } else {
            if (t.equals("str")) {
              valType=DT_STRING;
            }  else {
              throw new GetHarnessedException("prune: Unrecognized data type");
            }
          }
        }
        first = false;
      }
      for (int i=(iList.size() - 1); i > 0; i--) {
        /* or this
        switch(valType) {
        case DT_FLOAT:
        */
        if (iList.get(i).get(key) != val) {
          iList.remove(i);
        }
      }
    }
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
  }
}
