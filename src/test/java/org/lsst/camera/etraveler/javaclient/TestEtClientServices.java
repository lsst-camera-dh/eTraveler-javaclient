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
import java.util.Set;
import java.util.HashSet;
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
  public void setup() {  }
  
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
  
  @Test
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
  
  @Test
  public void testGetRunResults() 
    throws UnsupportedEncodingException, EtClientException, IOException {
    System.out.println("\n\nRunning testGetRunResults");
    boolean prodServer=false;
    System.out.println("prodServer is " + prodServer);
    boolean localServer=false;
    System.out.println("localServer is " + localServer);
    String appSuffix="-jrb";
    System.out.println("appSuffix is " + appSuffix);

    EtClientServices myService = new EtClientServices("Dev", null, prodServer, 
      localServer, appSuffix);
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

  @Test
  public void testGetRunSchemaResults() 
    throws UnsupportedEncodingException, EtClientException, IOException {
    boolean prodServer=false;
    System.out.println("\n\nRunning testGetRunSchemaResults");
    System.out.println("prodServer is " + prodServer);
    boolean localServer=false;
    System.out.println("localServer is " + localServer);
    String appSuffix="-jrb";
    System.out.println("appSuffix is " + appSuffix);
    EtClientServices myService =
      new EtClientServices("Dev", null, prodServer, localServer, appSuffix);
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
  
  @Test
  public void testGetResultsJH() 
    throws UnsupportedEncodingException, EtClientException, IOException {
    boolean prodServer=false;
    System.out.println("\n\nRunning testGetResultsJH");
    System.out.println("prodServer is " + prodServer);
    boolean localServer=false;
    System.out.println("localServer is " + localServer);
    String appSuffix="-jrb";
    System.out.println("appSuffix is " + appSuffix);
    EtClientServices myService =
      new EtClientServices("Prod", null, prodServer, localServer, appSuffix);
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
  
  @Test
  public void testGetResultsJH_filter() 
    throws UnsupportedEncodingException, EtClientException, IOException {
    boolean prodServer=false;
    System.out.println("\n\nRunning testGetResultsJH_filter");
    System.out.println("prodServer is " + prodServer);
    boolean localServer=false;
    System.out.println("localServer is " + localServer);
    String appSuffix="-jrb";
    System.out.println("appSuffix is " + appSuffix);
    EtClientServices myService =
      new EtClientServices("Prod", null, prodServer, localServer, appSuffix);
    String travelerName="SR-EOT-1";
    String hardwareType="ITL-CCD";
    String stepName="read_noise";
    String model="3800C";

    String function="getResultsJH";
    System.out.println("Arguments are travelerName=" + travelerName +
                       " hardwareType=" + hardwareType +
                       " stepName=" + stepName +
                       " schemaName=null" +
                       " model=" + model + 
                       ", function=" + function);
    System.out.println("And filter ('amp', 3) ");
    ArrayList<ImmutablePair <String, Object>> itemFilters = 
      new ArrayList<ImmutablePair <String, Object>>();
    ImmutablePair<String, Object> amp = new ImmutablePair("amp", 3);
    itemFilters.add(amp);
    
    try {
      Map<String, Object> results = 
        myService.getResultsJH(travelerName, hardwareType, stepName,
                               null, model, null, itemFilters, null);
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
  
  
  // Take it for now. A lot of output
  @Ignore @Test
  public void testGetResultsJH_schema() 
    throws UnsupportedEncodingException, EtClientException, IOException {
    boolean prodServer=false;
    System.out.println("\n\nRunning testGetResultsJH_schema");
    System.out.println("prodServer is " + prodServer);
    boolean localServer=false;
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
  
  // Don't need to run this regularly. Generates a log of output
  @Ignore @Test
  public void TestGetRunFilepaths()
    throws UnsupportedEncodingException, EtClientException, IOException {
    boolean prodServer=false;

    System.out.println("\n\nRunning testGetRunFilepaths");
    System.out.println("prodServer is " + prodServer);
    boolean localServer=false;
    System.out.println("localServer is " + localServer);
    String appSuffix="-jrb";
    System.out.println("appSuffix is " + appSuffix);
    EtClientServices myService =
      new EtClientServices("Prod", null, prodServer, localServer, appSuffix);

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


  //  @Ignore @Test
  @Test
  public void testGetFilepathsJH() 
throws UnsupportedEncodingException, EtClientException, IOException {
    boolean prodServer=false;
    System.out.println("\n\nRunning testGetFilepathsJH");
    System.out.println("prodServer is " + prodServer);
    boolean localServer=false;
    System.out.println("localServer is " + localServer);
    String appSuffix="-jrb";

    EtClientServices myService =
      new EtClientServices("Prod", null, prodServer, localServer, appSuffix);
    String travelerName="SR-EOT-1";
    String hardwareType="ITL-CCD";
    String stepName="preflight_acq";
    //String experimentSN="ITL-3800C-021";
    String experimentSN=null;
    HashSet<String> labels = new HashSet<String>();
    labels.add("SR_Grade:SR_SEN_Reserve");
    labels.add("SR_Grade:SR_SEN_Science");
    //String model="3800C";

    String function="getFilepathsJH";
    System.out.println("Arguments are travelerName=" + travelerName +
                       " hardwareType=" + hardwareType +
                       " stepName=" + stepName +
                       //" model=" + model +
                       // " experimentSN=" + experimentSN +
                       " labeled SR_SEN_Reserve or SR_SEN_Science " +
                       ", function=" + function);
    try {
      Map<String, Object> results = 
        myService.getFilepathsJH(travelerName, hardwareType, stepName,
                                 null, experimentSN, labels);
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
      new EtClientDataServer("LSST-CAMERA", EtClientDataServer.FRONTEND_DEV,
      "-jrb");
    
    HashMap<String, Object> results = null;
    
    //System.out.println("\nAll of 4689D");
    //results = (HashMap<String, Object>) dataServer.fetchRun("4689D", "Dev");
    //outputRun(results);      
    
    
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
    //System.out.println("\n Exercise ItemFilter: amp=3, step=fe55_raft_analysis");
    //results = (HashMap<String, Object>) dataServer.fetchRun("4689D", "Dev",
    //  "fe55_raft_analysis", null, itemFilters);
    //outputRun(results);

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

  @Test
  public void testGetActivity() throws UnsupportedEncodingException,
                                      EtClientException, IOException {
    boolean prodServer = false;
    boolean localServer = false;
    String appSuffix="-jrb";
    int activityId = 23000;
    String db="Prod";
    System.out.println("\n Exercise getActivity for db=" + db +
                       " and activityId=" + activityId);
    EtClientServices myService =
      new EtClientServices("Prod", null, prodServer, localServer, appSuffix);

    try {
      HashMap<String, Object> results =
        myService.getActivity(activityId);
      for (String key : results.keySet()) {
        System.out.println(key + ":" + results.get(key));
      }
    } catch (Exception ex) {
      System.out.println("Post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }

  @Test
  public void testGetRunActivities() throws UnsupportedEncodingException,
                                            EtClientException, IOException {
    boolean prodServer = false;
    boolean localServer = false;
    String appSuffix="-jrb";
    //String run="";
    int run = 4248;
    String db="Dev";
    System.out.println("\n Exercise getRunActivities for db=" + db +
                       " and run =" + run);
    EtClientServices myService =
      new EtClientServices(db, null, prodServer, localServer, appSuffix);

    try {
      ArrayList<HashMap<String, Object>> results =
        myService.getRunActivities(run);
      for (HashMap<String, Object> act : results) {
        System.out.println("Next activity data:");
        for (String key : act.keySet()) {
          System.out.println(key + ":" + act.get(key));
        }
      }
    } catch (Exception ex) {
      System.out.println("Post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }

  @Test
  public void testRunSummary() throws UnsupportedEncodingException,
                                      EtClientException, IOException {
    boolean prodServer = false;
    boolean localServer = false;
    String appSuffix="-jrb";
    String run = "4248D";
    String db="Dev";
    System.out.println("\n Exercise getRunSummary for db=" + db +
                       " and run=" + run);
    EtClientServices myService =
      new EtClientServices(db, null, prodServer, localServer, appSuffix);

    try {
      HashMap<String, Object> results =
        myService.getRunSummary(run);
      for (String key : results.keySet()) {
        System.out.println(key + ":" + results.get(key));
      }
    } catch (Exception ex) {
      System.out.println("Post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }

  @Test
  public void testGetInstances() throws UnsupportedEncodingException,
                                        EtClientException, IOException {
    boolean prodServer = false;
    boolean localServer = false;
    String appSuffix="-jrb";
    String htype = "boojum";
    String db="Raw";
    HashSet<String> labels = new HashSet<String>();
    labels.add("SnarkRandom:");
    System.out.println("\n Exercise getHardwareInstances for db=" + db +
                       " and hardware type=" + htype);
    System.out.println(" and group label wildcard 'SnarkRandom:' ");
    EtClientServices myService =
      new EtClientServices(db, null, prodServer, localServer, appSuffix);

    try {
      ArrayList<HashMap<String, Object> > results =
        myService.getHardwareInstances(htype, null, labels);
      for (HashMap<String, Object> cmp : results) {
        System.out.println("\n Next component:");
        for (String key : cmp.keySet()) {
          System.out.println(key + ":" + cmp.get(key));
        }
      }
    } catch (Exception ex) {
      System.out.println("Post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }

  @Test
  public void testComponentRuns() throws UnsupportedEncodingException,
                                         EtClientException, IOException {
    boolean prodServer = false;
    boolean localServer = false;
    String appSuffix="-jrb";
    String htype = "ITL-CCD";
    String expSN = "ITL-3800C-021";
    String travelerName="SR-EOT-1";
    String db="Prod";


    System.out.println("\n Exercise getComponentRuns for db=" + db +
                       " and hardware type=" + htype + " component " + expSN);
    EtClientServices myService =
      new EtClientServices(db, null, prodServer, localServer, appSuffix);


    try {
      Map<Integer, Object> results =
        myService.getComponentRuns(htype, expSN,null);
      for (Integer raid : results.keySet() ) {
        System.out.println("\n For run with raid " + raid);
        HashMap<String, Object> runInfo = (HashMap<String, Object>) results.get(raid);
        for (String key : runInfo.keySet()) {
          System.out.println(key + ":" + runInfo.get(key));
        }
      }
    } catch (Exception ex) {
      System.out.println("Post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }

    
    // Now do the same with travelerName arg
    System.out.println("\n Exercise getComponentRuns for db=" + db +
                       " and hardware type=" + htype + " component " + expSN
                       + " and traveler name=" + travelerName);
    myService = new EtClientServices(db, null, prodServer, localServer, appSuffix);
    
    try {
      Map<Integer, Object> results =
        myService.getComponentRuns(htype, expSN, travelerName);
      for (Integer raid : results.keySet() ) {
        System.out.println("\n For run with raid " + raid);
        HashMap<String, Object> runInfo = (HashMap<String, Object>) results.get(raid);
        for (String key : runInfo.keySet()) {
          System.out.println(key + ":" + runInfo.get(key));
        }
      }
    } catch (Exception ex) {
      System.out.println("Post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }
          
    
  @Test
  public void testManualRun() throws UnsupportedEncodingException,
                                     EtClientException, IOException {
    boolean prodServer = false;
    boolean localServer = false;
    String appSuffix="-jrb";
    //String run = "4276D";
    int run = 4276;
    String db="Dev";
    System.out.println("\n Exercise getManualRunResults for db=" + db +
                       " and run=" + run);
    EtClientServices myService =
      new EtClientServices(db, null, prodServer, localServer, appSuffix);

    try {
      HashMap<String, Object> results =
        myService.getManualRunResults(run, null);
      for (String key : results.keySet()) {
        if (!key.equals("steps")) {   // general run info
          System.out.println(key + ":" + results.get(key));
        }
      }
      printManualSteps((HashMap<String, Object>) results.get("steps"), false);
    } catch (Exception ex) {
      System.out.println("Post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }

  @Test
  public void testManualRunFilepaths() throws UnsupportedEncodingException,
                                              EtClientException, IOException {
    boolean prodServer = false;
    boolean localServer = false;
    String appSuffix="-jrb";
    //String run = "4276D";
    int run = 4864;
    String db="Prod";
    System.out.println("\n Exercise getManualRunFilepathss for db=" + db +
                       " and run=" + run);
    EtClientServices myService =
      new EtClientServices(db, null, prodServer, localServer, appSuffix);

    try {
      HashMap<String, Object> results =
        myService.getManualRunFilepaths(run, null);
      for (String key : results.keySet()) {
        if (!key.equals("steps")) {   // general run info
          System.out.println(key + ":" + results.get(key));
        }
      }
      printManualSteps((HashMap<String, Object>) results.get("steps"), false);
    } catch (Exception ex) {
      System.out.println("Post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }

  @Test
  public void testManualRunSignatures() throws UnsupportedEncodingException,
                                               EtClientException, IOException {
    boolean prodServer = false;
    boolean localServer = false;
    String appSuffix="-jrb";
    //String run = "4276D";
    //int run = 4955;
    int run = 5108;
    ArrayList<String> statuses = new ArrayList<>();
    statuses.add("success");
    statuses.add("inProgress");
    String db="Prod";
    System.out.println("\n Exercise getManualRunSignatures for db=" + db +
                       " and run=" + run);
    EtClientServices myService =
      new EtClientServices(db, null, prodServer, localServer, appSuffix);

    try {
      HashMap<String, Object> results =
        myService.getManualRunSignatures(run, null, statuses);
      for (String key : results.keySet()) {
        if (!key.equals("steps")) {   // general run info
          System.out.println(key + ":" + results.get(key));
        }
      }
      printManualSteps((HashMap<String, Object>) results.get("steps"), true);
    } catch (Exception ex) {
      System.out.println("Post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }

  @Test
  public void testManualStep() throws UnsupportedEncodingException,
                                     EtClientException, IOException {
    boolean prodServer = false;
    boolean localServer = false;
    String appSuffix="-jrb";
    String traveler="InputTraveler";
    String stepName="hasInputs";
    String htype="boojum";

    String db="Raw";
    System.out.println("\n Exercise getManualResultsStep for db=" + db +
                       ", traveler=" + traveler + ", step=" + stepName
                       + " and hardware type=" + htype);
    EtClientServices myService =
      new EtClientServices(db, null, prodServer, localServer, appSuffix);

    try {
      HashMap<String, Object> results =
        myService.getManualResultsStep(traveler, stepName, htype, null, null, null);
      for (String expSN : results.keySet()) {
        HashMap<String, Object> expData = (HashMap<String, Object>)
          results.get(expSN);
        System.out.println("\n\nFor component " + expSN);
        for (String key : expData.keySet()) {
          if (!key.equals("steps")) {   // general run info
            System.out.println(key + ":" + expData.get(key));
          }
        }
        printManualSteps((HashMap<String, Object>) expData.get("steps"), false);
      }
    } catch (Exception ex) {
      System.out.println("Post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }

  /* xx */
  @Test
  public void testManualFilepathsStep() throws UnsupportedEncodingException,
                                               EtClientException, IOException {
    boolean prodServer = false;
    boolean localServer = false;
    String appSuffix="-jrb";
    String traveler="SR-RSA-ASY-02";
    String stepName="SR-RSA-ASY-02_Analyze-Data-Run1";
    String htype="LCA-10753_RSA";

    String db="Prod";
    System.out.println("\n Exercise getManualFilepathsStep for db=" + db +
                       ", traveler=" + traveler + ", step=" + stepName
                       + " and hardware type=" + htype);
    EtClientServices myService =
      new EtClientServices(db, null, prodServer, localServer, appSuffix);

    try {
      HashMap<String, Object> results =
        myService.getManualFilepathsStep(traveler, stepName, htype,
                                         null, null, null);
      for (String expSN : results.keySet()) {
        HashMap<String, Object> expData = (HashMap<String, Object>)
          results.get(expSN);
        System.out.println("\n\nFor component " + expSN);
        for (String key : expData.keySet()) {
          if (!key.equals("steps")) {   // general run info
            System.out.println(key + ":" + expData.get(key));
          }
        }
        printManualSteps((HashMap<String, Object>) expData.get("steps"), false);
      }
    } catch (Exception ex) {
      System.out.println("Post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }

  @Test
  public void testManualSignaturesStep() throws UnsupportedEncodingException,
                                                EtClientException, IOException {
    boolean prodServer = false;
    boolean localServer = false;
    String appSuffix="-jrb";
    String traveler="NCR";
    String stepName="NCR_C_Final_disposition";
    String htype="e2v-CCD";
    ArrayList<String> statuses = new ArrayList<>();
    statuses.add("success");
    

    String db="Prod";
    System.out.println("\n Exercise getManualSignaturesStep for db=" + db +
                       ", traveler=" + traveler + ", step=" + stepName
                       + " and hardware type=" + htype);
    EtClientServices myService =
      new EtClientServices(db, null, prodServer, localServer, appSuffix);

    try {
      HashMap<String, Object> results =
        myService.getManualSignaturesStep(traveler, stepName, htype,
                                          null, null, null, statuses);
      for (String expSN : results.keySet()) {
        HashMap<String, Object> expData = (HashMap<String, Object>)
          results.get(expSN);
        System.out.println("\n\nFor component " + expSN);
        for (String key : expData.keySet()) {
          if (!key.equals("steps")) {   // general run info
            System.out.println(key + ":" + expData.get(key));
          }
        }
        printManualSteps((HashMap<String, Object>) expData.get("steps"), true);
      }
    } catch (Exception ex) {
      System.out.println("Post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }

  @Test
  public void testMissingSignatures()
    throws UnsupportedEncodingException, EtClientException, IOException {
    boolean prodServer = false;
    boolean localServer = false;
    String appSuffix="-jrb";

    String db="Dev";
    System.out.println("\n Exercise getMissingSignatures for db=" + db +
                       ", no arguments");
    EtClientServices myService =
      new EtClientServices(db, null, prodServer, localServer, appSuffix);

    try {
      HashMap<Integer, Object> results =
        myService.getMissingSignatures(null);
      for (Integer hid: results.keySet()) {
        HashMap<String, Object> expData = (HashMap<String, Object>)
          results.get(hid);
        System.out.println("\n\nFor component with id " + hid);
        /* NOTE:  Following is  certainly not quite right. 
               At the very least, printManualSteps won't do the
               right thing since step data is an array list (of maps),
               not a map
         */
        for (String run : expData.keySet()) {
          System.out.println("\nFor run " + run);
          HashMap<String, Object> runData =
            (HashMap<String, Object>) expData.get(run);
          for (String key : runData.keySet())  {
            if (!key.equals("steps")) {   // general run info
              System.out.println(key + ":" + runData.get(key));
            }
          }
          printMissingSigs((HashMap<String, Object>) runData.get("steps"));
        }
      }
    } catch (Exception ex) {
      System.out.println("Post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    } finally {
      myService.close();
    }
  }


  private static void outputRun(Map<String, Object> results ) {
       System.out.println("Outer map has following non-instance key/value pairs");
    for (String k : results.keySet() ) {
      if (!k.equals("steps") ) {
        System.out.println(k + ":" + results.get(k));
      }
    }

    Map<String, Object> stepMap;
    stepMap =
      (Map<String, Object>) results.get("steps");
    for (String name : stepMap.keySet() ) {
      System.out.println("\nStep name " + name);
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
  private static void printManualSteps(Map<String, Object> steps,
                                       boolean signatures) {
    for (String step : steps.keySet()) {
      System.out.println("Step name: " + step);
      HashMap<String, Object> inputs =
        (HashMap<String, Object>) steps.get(step);
      for (String iName : inputs.keySet()) {
        if (signatures) System.out.println("Signer request: " + iName);
        else System.out.println("Input name: " + iName);
        HashMap<String, Object> input =
          (HashMap<String, Object>) inputs.get(iName);
        for (String k : input.keySet() ) {
          System.out.println("For key '" + k + "' value is: " + input.get(k));
        }
      }
    }
  }
  private static void printMissingSigs(Map<String, Object> steps) {
    for (String step : steps.keySet()) {
      System.out.println("Step name: " + step);
      ArrayList<Object> inputs =
        (ArrayList<Object>) steps.get(step);
      for (Object sigObj : inputs) {
        HashMap<String, Object> sig = (HashMap<String, Object>) sigObj;
        for (String k : sig.keySet() ) {
          System.out.println("for key '" + k + "' value is: " + sig.get(k));
        }
      }
    }
  }
}
