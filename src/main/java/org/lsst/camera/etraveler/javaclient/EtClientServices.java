package org.lsst.camera.etraveler.javaclient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.HashMap;

public class EtClientServices  {
  private EtClient m_client=null;
  private String m_operator = "read_only";

  public EtClientServices() {
    m_client = new EtClient(null, null);
  }

  public EtClientServices(String db, String exp, boolean prodServer) {
    m_client = new EtClient(db, exp);
    m_client.setProdServer(prodServer);
  }
  public EtClientServices(String db, String exp, boolean prodServer,
                          boolean localServer) {
    m_client = new EtClient(db, exp);
    m_client.setProdServer(prodServer);
    if (localServer) {
      m_client.useLocalServer();
    }
  }
    

  public Map<String, Object> getRunInfo(int activityId) throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("activityId", activityId);
    Map<String, Object> results = m_client.execute("getRunInfo", args);
    return consumeAck(results);
  }

  public Map<String, Object> getManufacturerId(String experimentSN,
                                               String hardwareTypeName)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("experimentSN", experimentSN);
    args.put("hardwareTypeName", hardwareTypeName);
    Map<String, Object> results = m_client.execute("getManufacturerId", args);
    return consumeAck(results);
  }

  public Map<String, Object> getHardwareHierarchy(String experimentSN,
                                                  String hardwareTypeName)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("experimentSN", experimentSN);
    args.put("hardwareTypeName", hardwareTypeName);
    Map<String, Object> results =
      m_client.execute("getHardwareHierarchy", args);
    return consumeAck(results);
  }

  public Map<String, Object>
    getContainingHardwareHierarchy(String experimentSN, String hardwareTypeName)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("experimentSN", experimentSN);
    args.put("hardwareTypeName", hardwareTypeName);
    Map<String, Object> results =
      m_client.execute("getContainingHardware", args);
    return consumeAck(results);
  }

  public HashMap<String, Object> getRunResults(String run, String schemaName)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("run", run);
    if (schemaName != null) {
      args.put("schemaName", schemaName);
    }
    args.put("function", "getRunResults");
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object>) consumeAck(results).get("results");
  }

  public HashMap<String, Object> getRunFilepaths(String run, String stepName)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("run", run);
    if (stepName != null) {
      args.put("stepName", stepName);
    }
    args.put("function", "getRunFilepaths");
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object>) consumeAck(results).get("results");
  }
    
  public HashMap<String, Object>
    getResultsJH(String travelerName, String hardwareType, String schemaName,
                 String model, String experimentSN) 
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("travelerName", travelerName);
    args.put("hardwareType", hardwareType);
    args.put("schemaName", schemaName);
    if (model != null) {
      args.put("model", model);
    }
    if (experimentSN != null) {
      args.put("experimentSN", experimentSN);
    }
    args.put("function", "getResultsJH");
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object>) consumeAck(results).get("results");
  }

  public HashMap<String, Object>
    getFilepathsJH(String travelerName, String hardwareType, String stepName,
                 String model, String experimentSN) 
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("travelerName", travelerName);
    args.put("hardwareType", hardwareType);
    args.put("stepName", stepName);
    if (model != null) {
      args.put("model", model);
    }
    if (experimentSN != null) {
      args.put("experimentSN", experimentSN);
    }
    args.put("function", "getFilepathsJH");
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object>) consumeAck(results).get("results");
  }

    
  private Map<String, Object> consumeAck(Map<String, Object> results)
    throws EtClientException {
    if (results.get("acknowledge") == null) {
      results.remove("acknowledge");
      return results;
    } else {
      throw new EtClientException("Operation failed with error: " +
                                  results.get("acknowledge").toString());
    }
  }

  public void close() throws IOException {
    m_client.close();
    m_client = null;
  }
}
