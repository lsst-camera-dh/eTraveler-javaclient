package org.lsst.camera.etraveler.javaclient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.lsst.camera.etraveler.javaclient.utils.RunUtils;
  
/**
 * Class which knows how to send requests to eTraveler front-end to
 * retrieve database entries
 * @author jrb
 */
public class EtClientServices  {
  private EtClient m_client=null;
  private String m_operator = "read_only";
  private String m_appSuffix="";

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

  /**
   * Allows full specification of connection.  Constructors above
   * default one or more quantities
   * @param db    "Prod", "Dev", etc  Defaults above to "Prod"
   * @param exp      Defaults above to "LSST-CAM"
   * @param prodServer    Defaults to true
   * @param localServer    Defaults to false
   * @param appSuffix      Defaults to ""
   */
  public EtClientServices(String db, String exp, boolean prodServer,
                          boolean localServer, String appSuffix) {
    if (appSuffix != null) m_client = new EtClient(db, exp, appSuffix);
    else m_client = new EtClient(db,exp);
    m_client.setProdServer(prodServer);
    if (localServer) {
      m_client.useLocalServer();
    }
  }

  /**
   * Return information about the run containing specified activity
   * @param activityId
   * @return   Map returning run number (as string), rootActivityId
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public Map<String, Object> getRunInfo(int activityId)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("activityId", activityId);
    Map<String, Object> results = m_client.execute("getRunInfo", args);
    return consumeAck(results);
  }

  /**
   * Return manufacturer id for  specified component
   * @param experimentSN
   * @param hardwareTypeName
   * @return  Map with single key, value = manufacturer id
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public Map<String, Object> getManufacturerId(String experimentSN,
                                               String hardwareTypeName)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("experimentSN", experimentSN);
    args.put("hardwareTypeName", hardwareTypeName);
    Map<String, Object> results = m_client.execute("getManufacturerId", args);
    return consumeAck(results);
  }

  /**
   * Return Array of maps, each representing parent-child assembly relationship
   * of two components.  Each map has values corresponding to
   * level (top is level 0), relationship name, slot name, and, for each of
   * parent and child components, hardware type name, experiment SN and 
   * internal id
   * @param experimentSN
   * @param hardwareTypeName
   * @return Map with a single key, value as described above
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
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

  /**
   * Returns ArrayList of maps similar to getContainingHardwareHierarchy
   * but for ancestors of specified component
   * @param experimentSN
   * @param hardwareTypeName
   * @return
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
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

  /**
   * Return data produced by harnessed jobs in specified run
   * @param run    Required non-null
   * @param stepName  Return data only for this step (if non-null)
   * @param schemaName Retrun data only for this schema (if non-null)
   * @return A map with some overall run information and key "steps"
   * organizing harnessed job info by step (map keyed by stepName), 
   * schema (map keyed by schema name), schemaInstance (array list)
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object> getRunResults(String run, String stepName,
                                               String schemaName)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("run", run);
    if (schemaName != null) {
      args.put("schemaName", schemaName);
    }
    if (stepName != null) {
      args.put("stepName", stepName);
    }
    args.put("function", "getRunResults");
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object>) consumeAck(results).get("results");
  }

  /**
   * Return information concerning all files registered by harnessed jobs
   * in this run
   * @param run   Required non-null
   * @param stepName Return information only for this step (if non-null)
   * @return A map (keyed by stepname).  Values are arraylists of maps.
   * Information in low-level map includes virtual path, catalog key, etc.
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
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
  
  /**
   * Return data for components, run, step, schema as specified
   * @param travelerName   Must be non-null
   * @param hardwareType   Must be non-null
   * @param stepName       Must be non-null
   * @param schemaName  If non-null, fetch data only for this schema
   * @param model If non- null, fetch data only for hardware of this model
   * @param experimentSN If non-null, fetch data only for this component
   * @return Map keyed by experimentSN.  Value for each entry is similar in
   * structure to return from getRunResults
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object>
    getResultsJH(String travelerName, String hardwareType, String stepName,
                 String schemaName, String model, String experimentSN) 
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("travelerName", travelerName);
    args.put("hardwareType", hardwareType);
    args.put("stepName", stepName);
    if (stepName != null) {
      args.put("schemaName", schemaName);
    }
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

    /**
     * Fetch data as above. Then filter using @arg itemFilters.  
     * @param travelerName
     * @param hardwareType
     * @param stepName
     * @param schemaName
     * @param model
     * @param experimentSN
     * @param itemFilters list of (key, value) pairs.  If key is key for a
     * schema, throw out any instance of that schema which don't have
     * specified value
     * @return
     * @throws UnsupportedEncodingException
     * @throws IOException
     * @throws EtClientException 
     */
  public HashMap<String, Object>
    getResultsJH(String travelerName, String hardwareType, String stepName,
                 String schemaName, String model, String experimentSN,
                 ArrayList<ImmutablePair<String, Object>> itemFilters)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> results =
      getResultsJH(travelerName, hardwareType, stepName, schemaName,
                   model, experimentSN);
    if (itemFilters == null) return results;
    for (Object k : results.keySet()) {
      HashMap<String, Object> oneRun = (HashMap<String, Object>) results.get(k);
      HashMap<String, Object> steps = (HashMap<String, Object>) oneRun.get("steps");
      RunUtils.pruneRun(steps, itemFilters);
    }
    return results;
  }

  /**
   * Return filepath data for components, run, step as specified
   * @param travelerName Must be non-null
   * @param hardwareType Must be non-null
   * @param stepName     Must be non-null
   * @param model      If non-null return data only for components of this model
   * @param experimentSN If non-null return data only for this component
   * @return Map keyed by experimentSN.  Value for each entry is similar in
   * structure to return from getRunFilepaths
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
   
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
