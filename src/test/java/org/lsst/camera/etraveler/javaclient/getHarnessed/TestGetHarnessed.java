package org.lsst.camera.etraveler.javaclient.getHarnessed;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.Ignore;
import java.sql.Connection;
import java.sql.SQLException;

import java.util.Map;
import java.util.ArrayList;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

// import org.lsst.camera.etraveler.backend.exceptions.EtravelerException;

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
    String db="Prod";

    String dbUrl = prodUrl;

    String ro = db + "_ro";

    // dbUrl += "rd_lsst_cam";
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

  @Test
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

  @Test
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

  /**
   *
   * @throws GetHarnessedException
   * @throws SQLException
   */
  @Test
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
  
  
  @After
  public void after() throws SQLException {
    m_connect.close();
    m_connect= null;
  }
}
