package org.lsst.camera.etraveler.javaclient;

import org.lsst.camera.etraveler.javaclient.EtClientServices;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.*;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

// Could make handling output a little nicer but not as they stand
//import org.lsst.camera.etraveler.javaclient.getHarnessed.PerSchema;
//import org.lsst.camera.etraveler.javaclient.getHarnessed.PerStep;


import java.io.UnsupportedEncodingException;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class TestEtClientServices {

  private HashMap<String, Object> m_params;


  @Before
  public void setup() {

  }
  
  @Test
  public void testGetRunInfo() throws UnsupportedEncodingException,
                                      EtClientException, IOException {
    boolean prodServer = false;
    System.out.println("\nRunning testGetRunInfo");
    System.out.println("prodServer is " + prodServer);
    EtClientServices myService = new EtClientServices("Dev", null, prodServer);

    try {
      Map<String, Object> results = myService.getRunInfo(200);

      assertNotNull(results);
      for (String k: results.keySet() ) {
        Object v = results.get(k);
        if (v == null) {
          System.out.println("Key '" + k + "' has value null");
        } else {
          System.out.println("Key '" + k + "' has value: " + v.toString());
        }
      }
    } catch (Exception ex) {
      System.out.println("post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    }
    finally {
      myService.close();
    }
  }

  @Test
  public void testGetManufacturerId() throws UnsupportedEncodingException,
                                      EtClientException, IOException {

    boolean prodServer=true;
    System.out.println("\nRunning testGetManufacturerId");
    System.out.println("prodServer is " + prodServer);

    //EtClientServices myService = new EtClientServices("Dev", null, false, true);
    EtClientServices myService = new EtClientServices("Dev", null, prodServer);

    try {
      Map<String, Object> results =
        myService.getManufacturerId("E2V-CCD250-179", "e2v-CCD");

      assertNotNull(results);
      for (String k: results.keySet() ) {
        Object v = results.get(k);
        if (v == null) {
          System.out.println("Key '" + k + "' has value null");
        } else {
          System.out.println("Key '" + k + "' has value: " + v.toString());
        }
      }
    } catch (Exception ex) {
      System.out.println("post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    }
    finally {
      myService.close();
    }
  }

  
  @Ignore @Test
  public void testGetHardwareHierarchy() throws UnsupportedEncodingException,
                                                EtClientException, IOException {
    boolean prodServer=false;
    boolean localServer=false;
    System.out.println("Running testGetHardwareHierarchy");

    EtClientServices myService =
      new EtClientServices("Raw", null, prodServer, localServer);

    try {
      Map<String, Object> results =
        myService.getHardwareHierarchy("dessert_01", "dessert");

      assertNotNull(results);
      for (String k: results.keySet() ) {
        Object v = results.get(k);
        if (v == null) {
          System.out.println("Key '" + k + "' has value null");
        } else {
          System.out.println("Key '" + k + "' has value: " + v.toString());
        }
      }
      ArrayList< Map<String, Object> > rows =
        (ArrayList<Map<String, Object> >) results.get("hierarchy");
      for (Map <String, Object> row: rows) {
        for (String k: row.keySet() ) {
          System.out.print("Key '" + k + "': " + row.get(k).toString() + " ");
        }
        System.out.println(" ");
      }
    } catch (Exception ex) {
      System.out.println("post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    }
    finally {
      myService.close();
    }
  }
  
  // Temporarily ignore
  @Ignore @Test
  public void testGetRunResults() 
    throws UnsupportedEncodingException, EtClientException, IOException {
    System.out.println("\n\nRunning testGetRunResults");
    boolean prodServer=false;
    System.out.println("prodServer is " + prodServer);
    boolean localServer=false;
    System.out.println("localServer is " + localServer);

    EtClientServices myService = new EtClientServices("Dev", null, prodServer, localServer);
    String run="4689D";

    String function="getRunResults";
    System.out.println("Arguments are run=" + run + 
                       ", function=" + function);
    // ", schema=" + schname +", step=" + stepname +
    try {
      Map<String, Object> results = 
        myService.getRunResults(run, null, null);  // , stepname, schname );
      TestEtClientServices.outputRun(results);
    } catch (Exception ex) {
      System.out.println("failed with exception " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }

  }

  // Temporarily ignore
  @Ignore @Test
  public void testGetRunSchemaResults() 
    throws UnsupportedEncodingException, EtClientException, IOException {
    boolean prodServer=false;
    System.out.println("\n\nRunning testGetRunSchemaResults");
    System.out.println("prodServer is " + prodServer);
    boolean localServer=false;
    System.out.println("localServer is " + localServer);

    EtClientServices myService =
      new EtClientServices("Dev", null, prodServer, localServer);
    String run="4689D";
    String schname="fe55_raft_analysis";
    String function="getRunResults";
    System.out.println("Arguments are run=" + run + 
                       ", schema=" + schname +
                       ", step=null" + 
                       ", function=" + function);
    try {
      Map<String, Object> results = 
        myService.getRunResults(run, null, schname);
      TestEtClientServices.outputRun(results);
    } catch (Exception ex) {
      System.out.println("failed with exception " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }

  }

  // Temporarily ignore
  @Ignore @Test
  public void testGetResultsJH() 
    throws UnsupportedEncodingException, EtClientException, IOException {
    boolean prodServer=false;
    System.out.println("\n\nRunning testGetResultsJH");
    System.out.println("prodServer is " + prodServer);
    boolean localServer=false;
    System.out.println("localServer is " + localServer);

    EtClientServices myService =
      new EtClientServices("Prod", null, prodServer, localServer);
    String travelerName="SR-EOT-1";
    String hardwareType="ITL-CCD";
    String stepName="read_noise";
    String experimentSN="ITL-3800C-021";

    String function="getResultsJH";
    System.out.println("Arguments are travelerName=" + travelerName +
                       " hardwareType=" + hardwareType +
                       " stepName=" + stepName +
                       " schemaName=null" +
                       " experimentSN=" + experimentSN + 
                       ", function=" + function);
    try {
      Map<String, Object> results = 
        myService.getResultsJH(travelerName, hardwareType, stepName,
                               null, null, experimentSN);
      for (String cmp : results.keySet() ) {
        HashMap<String, Object> cmpResults =
          (HashMap<String, Object>) results.get(cmp);
        System.out.println("Results for " + cmp);
        TestEtClientServices.outputRun(cmpResults);
      }
    } catch (Exception ex) {
      System.out.println("failed with exception " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }
  
  // Temporarily reinstate to test appSuffix
  @Test
  public void testGetResultsJH_schema() 
    throws UnsupportedEncodingException, EtClientException, IOException {
    boolean prodServer=false;
    System.out.println("\n\nRunning testGetResultsJH_schema");
    System.out.println("prodServer is " + prodServer);
    boolean localServer=true;
    System.out.println("localServer is " + localServer);
    String appSuffix="-jrb";
    System.out.println("appSuffix is " + appSuffix);
    EtClientServices myService =
      new EtClientServices("Prod", null, prodServer, localServer, appSuffix);
    String travelerName="SR-EOT-1";
    String hardwareType="ITL-CCD";
    String stepName="read_noise";
    String schemaName="package_versions";
    String experimentSN="ITL-3800C-021";

    String function="getResultsJH";
    System.out.println("Arguments are travelerName=" + travelerName +
                       " hardwareType=" + hardwareType +
                       " stepName=" + stepName +
                       " schemaName=" + schemaName +
                       " experimentSN=" + experimentSN + 
                       ", function=" + function);
    try {
      Map<String, Object> results = 
        myService.getResultsJH(travelerName, hardwareType, stepName,
                               schemaName, null, experimentSN);
      for (String cmp : results.keySet() ) {
        HashMap<String, Object> cmpResults =
          (HashMap<String, Object>) results.get(cmp);
        System.out.println("Results for " + cmp);
        TestEtClientServices.outputRun(cmpResults);
      }
    } catch (Exception ex) {
      System.out.println("failed with exception " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }
  
  // Temporarily reinstate
  @Test
  public void TestGetRunFilepaths()
    throws UnsupportedEncodingException, EtClientException, IOException {
    boolean prodServer=false;

    System.out.println("\n\nRunning testGetRunFilepaths");
    System.out.println("prodServer is " + prodServer);
    boolean localServer=true;
    System.out.println("localServer is " + localServer);

    EtClientServices myService =
      new EtClientServices("Prod", null, prodServer, localServer);

    String run="72";
    String function="getRunFilepaths";
    try {
      Map<String, Object> results = 
        myService.getRunFilepaths(run, null);

      TestEtClientServices.outputRunFiles(results);
    } catch (Exception ex) {
      System.out.println("failed with exception " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }

  // Temporarily ignore
  @Ignore @Test
  public void testGetFilepathsJH() 
throws UnsupportedEncodingException, EtClientException, IOException {
    boolean prodServer=false;
    System.out.println("\n\nRunning testGetFilepathsJH");
    System.out.println("prodServer is " + prodServer);
    boolean localServer=false;
    System.out.println("localServer is " + localServer);


    EtClientServices myService =
      new EtClientServices("Prod", null, prodServer, localServer);
    String travelerName="SR-EOT-1";
    String hardwareType="ITL-CCD";
    String stepName="preflight_acq";
    String experimentSN="ITL-3800C-021";
    //String model="3800C";

    String function="getFilepathsJH";
    System.out.println("Arguments are travelerName=" + travelerName +
                       " hardwareType=" + hardwareType +
                       " stepName=" + stepName +
                       //" model=" + model +
                       " experimentSN=" + experimentSN + 
                       ", function=" + function);
    try {
      Map<String, Object> results = 
        myService.getFilepathsJH(travelerName, hardwareType, stepName,
                              null, experimentSN);
      //             model, null);
      for (String cmp : results.keySet() ) {
        HashMap<String, Object> cmpResults =
          (HashMap<String, Object>) results.get(cmp);
        System.out.println("\nResults for " + cmp);
        TestEtClientServices.outputRunFiles((Map<String, Object>)cmpResults.get("steps"));
      }
    } catch (Exception ex) {
      System.out.println("failed with exception " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }

  @Test
  public void testDataServer() 
  throws UnsupportedEncodingException, EtClientException, IOException {
    System.out.println("\n\nRunning testDataServer");
    EtClientDataServer dataServer = 
      new EtClientDataServer("LSST-CAMERA", EtClientDataServer.FRONTEND_LOCAL,
      "-jrb");
    
    
    HashMap<String, Object> results = null;
    
    System.out.println("\nAll of 4689D");
    results = (HashMap<String, Object>) dataServer.fetchRun("4689D", "Dev");
    outputRun(results);      
    
    
    System.out.println("\nKeep only step fe55_raft_acq");
    results = (HashMap<String, Object>) dataServer.fetchRun("4689D", "Dev", 
      "fe55_raft_acq", null, null);
    outputRun(results); 
    System.out.println("\nKeep only schema package_versions");
    results = (HashMap<String, Object>) dataServer.fetchRun("4689D", "Dev", 
      null, "package_versions", null);
      outputRun(results);
    System.out.println("\nKeep only step fe55_raft_acq, schema package_versions");
    results = (HashMap<String, Object>) dataServer.fetchRun("4689D", "Dev", 
      "fe55_raft_acq", "package_versions", null);
    outputRun(results); 

    ArrayList<ImmutablePair <String, Object>> itemFilters = 
      new ArrayList<ImmutablePair <String, Object>>();
    ImmutablePair<String, Object> amp = new ImmutablePair("amp", 3);
    itemFilters.add(amp);
    System.out.println("\n Exercise ItemFilter: amp=3, step=fe55_raft_analysis");
    results = (HashMap<String, Object>) dataServer.fetchRun("4689D", "Dev",
      "fe55_raft_analysis", null, itemFilters);
    outputRun(results);

    ImmutablePair<String, Object> slot = new ImmutablePair("slot", "S20");
    itemFilters.add(slot);
    System.out.println("\n Exercise ItemFilter: amp=3, slot=S20, step=fe55_raft_analysis");
    results = (HashMap<String, Object>) dataServer.fetchRun("4689D", "Dev",
      "fe55_raft_analysis", null, itemFilters);
    outputRun(results);
    
    System.out.println("\nAll of 4689D again, to make sure it's still there");
    results = (HashMap<String, Object>) dataServer.fetchRun("4689D", "Dev");
    outputRun(results);      
    
  }
 
  private static void outputRun(Map<String, Object> results ) {
       System.out.println("Outer map has following non-instance key/value pairs");
    for (String k : results.keySet() ) {
      if (!k.equals("steps") ) {
        System.out.println(k + ":" + results.get(k));
      }
    }

    //Map<String, Map<String, ArrayList <Map<String, Object> > > >schemaMap;
    Map<String, Object> stepMap;
    stepMap =
      (Map<String, Object>) results.get("steps");
    for (String name : stepMap.keySet() ) {
      System.out.println("Step name " + name);
      Map<String, Object> perStep = (Map<String, Object>) stepMap.get(name); 
      for (String schname : perStep.keySet()) {
        System.out.println("  Schema name " + schname);
        ArrayList< Map<String, Object> > instances = (ArrayList< Map<String, Object> >) perStep.get(schname);
        System.out.println("  Instance array is of length " + instances.size() );
        System.out.println("  Instance data for this step/schema:");
        for (Object obj : instances) {
          Map <String, Object> m = (Map <String, Object>) obj;
          System.out.println(m);
        }
        System.out.println(" ");
      }
    }
  }

  private static void outputRunFiles(Map<String, Object> results) {
    for (String name : results.keySet() ) {
      System.out.println("Step name " + name);
      ArrayList<Map<String, Object> > instances =
        (ArrayList<Map<String, Object> >) results.get(name);
      System.out.println("  Instance array is of length " + instances.size() );
      System.out.println("  Filepath data for this step:");
      for (Object obj : instances) {
        Map <String, Object> m = (Map <String, Object>) obj;
        System.out.println(m);
      }
      System.out.println(" ");
    }
  }

}
