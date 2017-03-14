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
    System.out.println("");
    Map<String, Object> first = (Map<String, Object>) results.get(cmps.get(0));
    //System.out.println("Component map has the following key:value pairs ");
    System.out.println("Component map has the following keys ");
    for (String k : first.keySet() ) {
      //System.out.print(" " + k + ":" + first.get(k));
      System.out.print(" " + k);
    }
    System.out.println("");

    ArrayList<Map <String, Object> > instances =
      (ArrayList<Map <String, Object> >) first.get("instances");
    System.out.println("Component " + cmps.get(0) + " has " + instances.size()
                       + " instances ");
    System.out.println("Contents of instance 0:");
    
    System.out.print("{");
    for (String k : instances.get(0).keySet()) {
      System.out.print(k + " : " + instances.get(0).get(k) + ", ");
    }
    System.out.println("}");
    
    System.out.println("\nContents of instance 1:");
    
    System.out.print("{");
    for (String k : instances.get(1).keySet()) {
      System.out.print(k + " : " + instances.get(1).get(k) + ", ");
    }
    System.out.println("}");
    
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
    Map<String, Object> first = (Map<String, Object>) results.get(cmps.get(0));
    System.out.println("Component map has the following keys ");
    for (String k : first.keySet() ) {
      System.out.print(" " + k);
    }
    System.out.println("");

    ArrayList<Map <String, Object> > instances =
      (ArrayList<Map <String, Object> >) first.get("instances");
    System.out.println("Component " + cmps.get(0) + " has " + instances.size()
                       + " instances ");
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

    Pair<String, Object> filter =
      new ImmutablePair<String, Object>("amp", 3);
    Map<String, Object> results =
      getHarnessed.getResultsJH("SR-EOT-1", "ITL-CCD", "read_noise",
                                "3800C", null, filter);
    System.out.println("Found results for these components: ");
    ArrayList<String> cmps = new ArrayList<String>();
    for (String expSN : results.keySet() ) {
      cmps.add(expSN);
      System.out.print(" " + expSN);
    }
    System.out.println("");
    Map<String, Object> first = (Map<String, Object>) results.get(cmps.get(0));
    System.out.println("Component map has the following keys: ");
    for (String k : first.keySet() ) {
      System.out.print(" " + k);
    }

    System.out.println("");

    ArrayList<Map <String, Object> > instances =
      (ArrayList<Map <String, Object> >) first.get("instances");
    System.out.println("Component " + cmps.get(0) + " has " + instances.size()
                       + " instances ");
  }

  @Ignore @Test
  public void getRaftOneCCD() throws GetHarnessedException, SQLException {

    System.out.println("Running test getRaftOneCCD");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    Pair<String, Object> filter =
      new ImmutablePair<String, Object>("sensor_id", "ITL-3800C-102-Dev");
    Map<String, Object> results =
      getHarnessed.getResultsJH("SR-RTM-EOT-03", "LCA-11021_RTM",
                                "fe55_raft_analysis",
                                null, "LCA-11021_RTM-004_ETU2-Dev", filter);
    printJHResults(results);
  }

  @Ignore @Test
  public void getRaftVersions() throws GetHarnessedException, SQLException {

    System.out.println("Running test getRaftVersions");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    //Pair<String, Object> filter =
    //  new ImmutablePair<String, Object>("sensor_id", "ITL-3800C-102-Dev");
    Map<String, Object> results =
      getHarnessed.getResultsJH("SR-RTM-EOT-03", "LCA-11021_RTM",
                                "package_versions",
                                null, "LCA-11021_RTM-004_ETU2-Dev", null);
    printJHResults(results);
  }

  @Ignore @Test
  public void getRaftOneAmp() throws GetHarnessedException, SQLException {

    System.out.println("Running test getRaftOneAmp");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    Pair<String, Object> filter =
      new ImmutablePair<String, Object>("amp", 3);
    Map<String, Object> results =
      getHarnessed.getResultsJH("SR-RTM-EOT-03", "LCA-11021_RTM",
                                "fe55_raft_analysis",
                                null, "LCA-11021_RTM-004_ETU2-Dev", filter);
    printJHResults(results);
  }

  @Ignore @Test
  public void getRaftRun()  throws GetHarnessedException, SQLException {

    System.out.println("Running test getRaftRun");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    Pair<String, Object> filter =
      new ImmutablePair<String, Object>("sensor_id", "ITL-3800C-102-Dev");
    Map<String, Object> results =
      getHarnessed.getRunResults("4689D", "fe55_raft_analysis", filter);
    printRunResults(results);
  }

  @Test
  public void getVersionsRun() throws GetHarnessedException, SQLException {

    System.out.println("Running test getVersionsRun");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    Map<String, Object> results =
      getHarnessed.getRunResults("4689D", "package_versions", null);
    printRunResultsAll(results);
  }
  
  @Test
  public void getAllRun() throws GetHarnessedException, SQLException {

    System.out.println("Running test getAllRun");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    Map<String, Object> results =
      getHarnessed.getRunResults("4689D", null);
    printRunResultsAll(results);
  }

@Test
  public void getAllRunFiltered() throws GetHarnessedException, SQLException {

    System.out.println("Running test getAllRunRiltered");
    GetHarnessedData getHarnessed = new GetHarnessedData(m_connect);

    Pair<String, Object> filter =
      new ImmutablePair<String, Object>("sensor_id", "ITL-3800C-102-Dev");
    Map<String, Object> results =
      getHarnessed.getRunResults("4689D", filter);
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
    System.out.println("Hardware id: " + first.get("hid"));
    System.out.println("Root activity id: " + first.get("raid"));

    ArrayList < Map<String, Object> > instances =
      (ArrayList <Map <String, Object> > ) first.get("instances");
    System.out.println("Instance array is of length " + instances.size() );
    System.out.println("Instance data for this component:");
    for (Map <String, Object> m : instances) {
      System.out.println(m); System.out.println(" ");
    }
    
  }

  private void printRunResults(Map<String, Object> results) {
    System.out.println("Outer map has following non-instance key/value pairs");
    for (String k : results.keySet() ) {
      if (!k.equals("instances") ) {
        System.out.println(k + ":" + results.get(k));
      }
    }
    ArrayList < Map<String, Object> > instances =
      (ArrayList <Map <String, Object> > ) results.get("instances");
    System.out.println("Instance array is of length " + instances.size() );
    System.out.println("Instance data for this schema:");
    for (Map <String, Object> m : instances) {
      System.out.println(m); System.out.println(" ");
    }
  }

  /* For all-schema data */
  private void printRunResultsAll(Map<String, Object> results) {
    System.out.println("Outer map has following non-instance key/value pairs");
    for (String k : results.keySet() ) {
      if (!k.equals("schemas") ) {
        System.out.println(k + ":" + results.get(k));
      }
    }

    //Map<String, Map<String, ArrayList <Map<String, Object> > > >schemaMap;
    Map<String, PerSchema > schemaMap;
    schemaMap =
      (Map<String, PerSchema>) results.get("schemas");
    //      (Map<String, Map<String, ArrayList <Map<String, Object> > > >) results.get("schemas");
    for (String name : schemaMap.keySet() ) {
      System.out.println("Schema name " + name);
      //Map<String, ArrayList < Map<String, Object> > > pnameMaps =
      //  (Map<String, ArrayList <Map <String, Object> > > ) schemaMap.get(name);
      PerSchema perSchema = schemaMap.get(name); 
      //for (String pname : pnameMaps.keySet()) {
      //for (String pname : pnameMaps.keySet()) {
      for (String pname : perSchema.keySet()) {
        System.out.println("Step name " + pname);
        ArrayList<HashMap <String, Object> > instances =
          perSchema.get(pname).getArrayList();
        //(ArrayList<Map <String, Object> > ) pnameMaps.get(pname);
        System.out.println("Instance array is of length " + instances.size() );
        System.out.println("Instance data for this schema:");
        for (Map <String, Object> m : instances) {
          System.out.println(m); System.out.println(" ");
        }
      }
    }
  }

}
