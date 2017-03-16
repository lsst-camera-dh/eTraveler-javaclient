package org.lsst.camera.etraveler.javaclient.getHarnessed;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.Ignore;
import java.sql.Connection;
import java.sql.SQLException;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

// Other applications will need imports
// import org.lsst.camera.etravler.javaclient.getHarnessed.GetHarnessedData;
// import org.lsst.camera.etravler.javaclient.getHarnessed.GetHarnessedException;
// import org.lsst.camera.etravler.javaclient.getHarnessed.PerSchema;
// import org.lsst.camera.etravler.javaclient.getHarnessed.PerStep;


// Might move this to general-purpose utility to open connection
import java.sql.DriverManager;

import org.srs.web.base.db.ConnectionManager;

import static org.junit.Assert.*;

public class TestGetHarnessed {

  private Connection m_connect = null;

  @Before
  public void setup() throws GetHarnessedException, SQLException {
    String nonprodUrl
      = "jdbc:mysql://mysql-dev01.slac.stanford.edu:3307/";
    String prodUrl="jdbc:mysql://mysql-node03.slac.stanford.edu/";
    //String db="Prod";
    String db="Dev";
    String dbUrl = nonprodUrl;

    String ro = db + "_ro";

    String username = System.getProperty(ro + ".username");
    if (username == null)
      throw new GetHarnessedException("setup: Unable to get username property");
    String pwd = System.getProperty(ro + ".pwd");
    if (pwd == null)
      throw new GetHarnessedException("setup: Unable to get pwd property");
    String dbname = System.getProperty(ro + ".dbname");
    if (dbname == null)
      throw new GetHarnessedException("setup: Unable to get dbname property");
    else System.out.println("Found dbname=" + dbname);
    dbUrl += dbname;
    
    m_connect = DriverManager.getConnection(dbUrl, username, pwd);
    if (m_connect == null) System.out.println("No good connection");
  }

  @Ignore @Test
  /**
     Get data for one CCD, one schema
   */
  public void getOne() throws GetHarnessedException, SQLException {
    System.out.println("Running test getOne");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    Map<String, Object> results =
      getHarnessed.getResultsJH("SR-EOT-1", "ITL-CCD", "read_noise",
                                null, "ITL-3800C-021", null);
    System.out.println("Found results for these components: ");
    ArrayList<String> cmps = new ArrayList<String>();
    for (String expSN : results.keySet() ) {
      cmps.add(expSN);
      System.out.print(" " + expSN);
    }


    for (Object expObject : results.values() ) {
      HashMap<String, Object> expMap = (HashMap<String, Object>) expObject;
      printRunResultsAll(expMap);
    }
  }

  @Ignore @Test
  /**
     Get data for all CCDs with specified model, one schema
   */
  public void getModel() throws GetHarnessedException, SQLException {

    System.out.println("Running test getModel");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    Map<String, Object> results =
      getHarnessed.getResultsJH("SR-EOT-1", "ITL-CCD", "read_noise",
                                "3800C", null, null);
    System.out.println("Found results for these components: ");
    ArrayList<String> cmps = new ArrayList<String>();
    for (String expSN : results.keySet() ) {
      cmps.add(expSN);
      System.out.print(" " + expSN);
    }
    System.out.println("");
    int iExp = 0;
    for (Object expObject : results.values() ) {
      HashMap<String, Object> expMap = (HashMap<String, Object>) expObject;
      printRunResultsAll(expMap);
      iExp++;
      if (iExp > 3) break;
    }
  }

  /**
   *
   * @throws GetHarnessedException
   * @throws SQLException
   */
  @Ignore @Test
  /**
     Get data for all CCDs with specified model, one schema, filter on "amp"
   */
  public void getModelAmp3() throws GetHarnessedException, SQLException {

    System.out.println("Running test getModelAmp3");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);
    String tname="SR-EOT-1";
    String htype="ITL-CCD";
    String schname="read_noise";
    String model="3800C";
    Pair<String, Object> filter =
      new ImmutablePair<String, Object>("amp", 3);
    Map<String, Object> results =
      getHarnessed.getResultsJH(tname, htype, schname,
                                model, null, filter);

    printJHResults(results);
  }

  @Test
  public void getRaftOneCCD() throws GetHarnessedException, SQLException {

    System.out.println("Running test getRaftOneCCD");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    String tname="SR-RTM-EOT-03";
    String htype="LCA-11021_RTM";
    String schname="fe55_raft_analysis";
    String expSN="LCA-11021_RTM-004_ETU2-Dev";
    Pair<String, Object> filter =
      new ImmutablePair<String, Object>("sensor_id", "ITL-3800C-102-Dev");
    System.out.println("Calling getResultsJH with ");
    System.out.println("  traveler name = " + tname);
    System.out.println("  htype name    = " + htype);
    System.out.println("  schema name   = " + schname);
    System.out.println("  experimentSN  = " + expSN);
    System.out.println("  filter        = (" + filter.getLeft() + ", " + filter.getRight() + ")");
    Map<String, Object> results =
      getHarnessed.getResultsJH(tname, htype, schname, null, expSN, filter);
    printJHResults(results);
  }

  @Test
  public void getRaftVersions() throws GetHarnessedException, SQLException {

    String tname="SR-RTM-EOT-03";
    String htype="LCA-11021_RTM";
    String schname="package_versions";
    String expSN="LCA-11021_RTM-004_ETU2-Dev";
      
    System.out.println("Running test getRaftVersions");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    System.out.println("Calling getResultsJH with ");
    System.out.println("  traveler name = " + tname);
    System.out.println("  htype name    = " + htype);
    System.out.println("  schema name   = " + schname);
    System.out.println("  experimentSN  = " + expSN);
    
    Map<String, Object> results =
      getHarnessed.getResultsJH(tname, htype, schname, null, expSN, null);

    printJHResults(results);
  }

  @Test
  public void getRaftOneAmp() throws GetHarnessedException, SQLException {
    String tname="SR-RTM-EOT-03";
    String htype="LCA-11021_RTM";
    String schname="fe55_raft_analysis";
    String expSN="LCA-11021_RTM-004_ETU2-Dev";
    
    System.out.println("Running test getRaftOneAmp");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    Pair<String, Object> filter =
      new ImmutablePair<String, Object>("amp", 3);

    System.out.println("Calling getResultsJH with ");
    System.out.println("  traveler name = " + tname);
    System.out.println("  htype name    = " + htype);
    System.out.println("  schema name   = " + schname);
    System.out.println("  experimentSN  = " + expSN);
    System.out.println("  filter        = (" + filter.getLeft() + ", " + filter.getRight() + ")");

    Map<String, Object> results =
      getHarnessed.getResultsJH(tname, htype, schname, null, expSN, filter);
    printJHResults(results);
  }

  @Test
  public void getRaftRun()  throws GetHarnessedException, SQLException {

    String run="4689D";
    String schname="fe55_raft_analysis";
    System.out.println("Running test getRaftRun");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    Pair<String, Object> filter =
      new ImmutablePair<String, Object>("sensor_id", "ITL-3800C-102-Dev");
    
    System.out.println("Calling getRunResults with ");
    System.out.println("  run           = " + run);
    System.out.println("  schema name   = " + schname);
    System.out.println("  filter        = (" + filter.getLeft() + ", " + filter.getRight() + ")");
    
    Map<String, Object> results =
      getHarnessed.getRunResults(run, schname, filter);
    printRunResultsAll(results);
  }

  @Ignore @Test
  public void getVersionsRun() throws GetHarnessedException, SQLException {

    System.out.println("Running test getVersionsRun");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    Map<String, Object> results =
      getHarnessed.getRunResults("4689D", "package_versions", null);
    printRunResultsAll(results);
  }
  
  @Ignore @Test
  public void getAllRun() throws GetHarnessedException, SQLException {

    String run="4689D";

    
    System.out.println("Running test getAllRun");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    System.out.println("Calling getRunResults with ");
    System.out.println("  run           = " + run);
    
    Map<String, Object> results =
      getHarnessed.getRunResults("4689D", null);
    printRunResultsAll(results);
  }

  @Test
  public void getAllRunFiltered() throws GetHarnessedException, SQLException {

    String run="4689D";
    System.out.println("Running test getAllRunRiltered");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    Pair<String, Object> filter =
      new ImmutablePair<String, Object>("sensor_id", "ITL-3800C-102-Dev");
    System.out.println("Calling getRunResults with ");
    System.out.println("  run           = " + run);
    System.out.println("  filter        = (" + filter.getLeft() + ", " + filter.getRight() + ")");

    Map<String, Object> results =
      getHarnessed.getRunResults(run, filter);
    printRunResultsAll(results);
  }
  
    
  @After
  public void after() throws SQLException {
    m_connect.close();
    m_connect= null;
  }
  
  private void printJHResults(Map<String, Object> results) {
        System.out.println("Found results for these components: ");
    ArrayList<String> cmps = new ArrayList<String>();
    for (String expSN : results.keySet() ) {
      cmps.add(expSN);
      System.out.print(" " + expSN);
    }
    System.out.println("");

    Map<String, Object> first = (Map<String, Object>) results.get(cmps.get(0));
    System.out.println("Component map has the following keys ");
    for (String k : first.keySet() ) {
      System.out.println(" " + k);
    }

    for (Object expObject : results.values() ) {
      HashMap<String, Object> expMap = (HashMap<String, Object>) expObject;
      printRunResultsAll(expMap);
    }

  }

  /* For all-schema data */
  private void printRunResultsAll(Map<String, Object> results) {
    System.out.println("Outer map has following non-instance key/value pairs");
    for (String k : results.keySet() ) {
      if (!k.equals("steps") ) {
        System.out.println(k + ":" + results.get(k));
      }
    }

    //Map<String, Map<String, ArrayList <Map<String, Object> > > >schemaMap;
    Map<String, PerStep > stepMap;
    stepMap =
      (Map<String, PerStep>) results.get("steps");
    for (String name : stepMap.keySet() ) {
      System.out.println("Step name " + name);
      PerStep perStep = stepMap.get(name); 
      for (String schname : perStep.keySet()) {
        System.out.println("  Schema name " + schname);
        ArrayList<HashMap <String, Object> > instances =
          perStep.get(schname).getArrayList();
        System.out.println("  Instance array is of length " + instances.size() );
        System.out.println("  Instance data for this step/schema:");
        for (Map <String, Object> m : instances) {
          System.out.println(m);
        }
        System.out.println(" ");
      }
    }
  }

}
