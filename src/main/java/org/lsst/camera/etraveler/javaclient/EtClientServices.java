package org.lsst.camera.etraveler.javaclient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
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
   * Returns ArrayList of maps similar to getHardwareHierarchy
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
    Map<String, Object> results =
      (Map<String, Object>) m_client.execute("getResults", args);
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
    HashSet<String> emptySet = new HashSet<String>();
    return getResultsJH(travelerName, hardwareType, stepName, schemaName,
                        model, experimentSN, emptySet);
  }  
  /**
   * Return data for components, run, step, schema as specified
   * @param travelerName   Must be non-null
   * @param hardwareType   Must be non-null
   * @param stepName       Must be non-null
   * @param schemaName  If non-null, fetch data only for this schema
   * @param model If non- null, fetch data only for hardware of this model
   * @param experimentSN If non-null, fetch data only for this component
   * @param hardwareLabels Set of labels or group wildcard
   *   (e.g. "grpName:labelName" or "grpName:") If non-null return information only for
   *   components have at least one of the labels
   * @return Map keyed by experimentSN.  Value for each entry is similar in
   * structure to return from getRunResults
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object>
    getResultsJH(String travelerName, String hardwareType, String stepName,
                 String schemaName, String model, String experimentSN,
                 Set<String> hardwareLabels) 
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
    if (hardwareLabels != null) {
      if (!hardwareLabels.isEmpty()) args.put("hardwareLabels", hardwareLabels);
    }
    args.put("function", "getResultsJH");
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object>) consumeAck(results).get("results");
  }

    /** 
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
    return getResultsJH(travelerName, hardwareType, stepName, schemaName,
                        model, experimentSN, itemFilters, null);
  }
  
  /** 
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
   * @param hardwareLabels Set of labels or group wildcard
   *   (e.g. "grpName:labelName" or "grpName:") If non-null return information only for
   *   components have at least one of the labels
   * @return
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object>
    getResultsJH(String travelerName, String hardwareType, String stepName,
                 String schemaName, String model, String experimentSN,
                 ArrayList<ImmutablePair<String, Object>> itemFilters,
                 Set<String> hardwareLabels)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> results =
      getResultsJH(travelerName, hardwareType, stepName, schemaName,
                   model, experimentSN, hardwareLabels);
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
    return getFilepathsJH(travelerName, hardwareType, stepName, model, experimentSN, null);
  }
  
  /**
   * Return filepath data for components, run, step as specified
   * @param travelerName Must be non-null
   * @param hardwareType Must be non-null
   * @param stepName     Must be non-null
   * @param model      If non-null return data only for components of this model
   * @param experimentSN If non-null return data only for this component
   * @param hardwareLabels Set of labels or group wildcard
   *   (e.g. "grpName:labelName" or "grpName:") If non-null return information only for
   *   components have at least one of the labels
   * @return Map keyed by experimentSN.  Value for each entry is similar in
   * structure to return from getRunFilepaths
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object>
    getFilepathsJH(String travelerName, String hardwareType, String stepName,
                   String model, String experimentSN, Set<String> hardwareLabels) 
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
    if (hardwareLabels != null) {
      args.put("hardwareLabels", hardwareLabels);
    }

    args.put("function", "getFilepathsJH");
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object>) consumeAck(results).get("results");
  }

  /**
   * Return information about the specified activity
   * @param activityId
   * @return Map returning process name, activity id, begin and end times,
   *         status    
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */  
  public HashMap<String, Object> getActivity(int activityId)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    Integer iAct = new Integer(activityId);
    args.put("activityId", iAct.toString());
    args.put("function", "getActivity");
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object>) consumeAck(results).get("results");
  }

  /**
   * Return information about activities in the specified run
   * @param run  (int)
   * @return array list of maps.  See getActivity doc. for contents
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public ArrayList<HashMap<String, Object> > getRunActivities(int run)
    throws UnsupportedEncodingException, IOException, EtClientException {
    Integer runI = new Integer(run);
    return getRunActivities(runI.toString());
  }

  /**
   * Return information about activities in the specified run
   * @param run  (String)
   * @return array list of maps.  See getActivity doc. for contents
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public ArrayList<HashMap<String, Object> > getRunActivities(String run)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("run", run);
    args.put("function", "getRunActivities");    
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (ArrayList<HashMap<String, Object> >)
      consumeAck(results).get("results");
  }
    
  /**
   * Return information about the specified run
   * @param run  (int)
   * @return map with keys traveler name, traveler version, run number (int),
   *         run number (String), root activity id, hardware type,
   *         experimentSN, begin and end times, run status
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object>  getRunSummary(int run)
    throws UnsupportedEncodingException, IOException, EtClientException {
    Integer runI = new Integer(run);
    return getRunSummary(runI.toString());
  }

  /**
   * Return information about the specified run
   * @param run  (String)
   * @return map with keys traveler name, traveler version, run number (int),
   *         run number (String), root activity id, hardware type,
   *         experimentSN, begin and end times, run status
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object>  getRunSummary(String run)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("run", run);
    args.put("function", "getRunSummary");    
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object> )
      consumeAck(results).get("results");
  }

  /**
   * Return information about hardware components of specified type
   * @param  hardwareType
   * @param  experimentSN (null for all)
   * @return list of maps with keys 
   *         experimentSN, model, manufacturer, manufacturerId,
   *         remarks, status
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public ArrayList<HashMap<String, Object> >
    getHardwareInstances(String hardwareType, String experimentSN)
    throws UnsupportedEncodingException, IOException, EtClientException {
    return getHardwareInstances(hardwareType, experimentSN, null);
  }
  /**
   * Return information about hardware components of specified type
   * @param  hardwareType
   * @param  experimentSN (null for all)
   * @param  hardwareLabels 

   * @return list of maps with keys 
   *         experimentSN, model, manufacturer, manufacturerId,
   *         remarks, status
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public ArrayList<HashMap<String, Object> >
    getHardwareInstances(String hardwareType, String experimentSN,
                          Set<String> hardwareLabels)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("hardwareType", hardwareType);
    if (experimentSN != null) {
      args.put("experimentSN", experimentSN);
    }
    if (hardwareLabels != null) {
      args.put("hardwareLabels", hardwareLabels);
    }
    args.put("function", "getHardwareInstances");    
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (ArrayList<HashMap<String, Object> > )
      consumeAck(results).get("results");
  }
  /**
   * Return information about runs executed on a particular component.  Optionally
   * filter by traveler name
   * @param hardwareType (String)  required non-null
   * @param experimentSN (String) required non-null
   * @param travelerName (String) if null, return info for all travelers
   * @return Map indexed by rootActivityId.  Value for each key is another map
   *          containing information pertaining to that run  
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public Map<Integer, Object> getComponentRuns(String hardwareType, String experimentSN,
                                               String travelerName)  
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("hardwareType", hardwareType);
    args.put("experimentSN", experimentSN);
    if (travelerName != null) {
      args.put("travelerName", travelerName);
    }
    args.put("function", "getComponentRuns");    
    Map<String, Object> results =
      (Map<String, Object>) m_client.execute("getResults", args);
    Map<String, Object> justResults =
      (Map<String, Object>) consumeAck(results).get("results");

    // Now make a map with Integer keys out of this
    HashMap<Integer, Object> intKeyMap = new HashMap<>();
    for (String k : justResults.keySet()) {
      intKeyMap.put(new Integer(k), justResults.get(k));
    }
    return intKeyMap;
  }

  /**
   * Return information about manual inputs for the specified run
   * @param run  (String)
   * @return map with scalar values for information about the run
   *         overall (traveler name, traveler version, hardware type,
   *         experimentSN, subsystem, run number, begin and end times,
   *         run status) and the key 'steps' whose value is a map
   *         keyed by step name.   Value for each step is again a 
   *         map, keyed by manual input name.   And that value is
   *         yet another map with keys datatype, units, activityId,
   *         isOptional (0 for false, 1 for true) and value.   
   *         All manual inputs except those of type
   *         filepath or signature are included in the output
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object>
    getManualRunResults(String run, String stepName)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("run", run);
    args.put("function", "getManualRunResults");
    if (stepName != null) args.put("stepName", stepName);
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object> )
      consumeAck(results).get("results");
  }

  /**
   * Return information about manual inputs for the specified run
   * @param run  (int)
   * @param stepName (String) Restrict to specified step.  null for all
   * @return map with scalar values for information about the run
   *         overall (traveler name, traveler version, hardware type,
   *         experimentSN, subsystem, run number, begin and end times,
   *         run status) and the key 'steps' whose value is a map
   *         keyed by step name.   Value for each step is again a 
   *         map, keyed by manual input name.   And that value is
   *         yet another map with keys datatype, units, activityId,
   *         isOptional (0 for false, 1 for true) and value.   
   *         All manual inputs except those of type
   *         filepath or signature are included in the output
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object>  getManualRunResults(int run, String stepName)
    throws UnsupportedEncodingException, IOException, EtClientException {
    Integer iRun = new Integer(run);
    return getManualRunResults(iRun.toString(), stepName);
  }

  /**
   * Return information about manual filepath inputs for the specified run
   * @param run  (String)
   * @param stepName (String) Restrict to specified step.  null for all
   * @return map with scalar values for information about the run
   *         overall (traveler name, traveler version, hardware type,
   *         experimentSN, subsystem, run number, begin and end times,
   *         run status) and the key 'steps' whose value is a map
   *         keyed by step name.   Value for each step is again a 
   *         map, keyed by manual input name.   And that value is
   *         yet another map with keys activityId, virtualPath, catalogKey
   *         and isOptional (0 for false, 1 for true).   
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object>
    getManualRunFilepaths(String run, String stepName)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("run", run);
    args.put("function", "getManualRunFilepaths");
    if (stepName != null) args.put("stepName", stepName);
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object> )
      consumeAck(results).get("results");
  }

  /**
   * Return information about manual filepath for the specified run
   * @param run  (int)
   * @param stepName (String) Restrict to specified step.  null for all
   * @return map with scalar values for information about the run
   *         overall (traveler name, traveler version, hardware type,
   *         experimentSN, subsystem, run number, begin and end times,
   *         run status) and the key 'steps' whose value is a map
   *         keyed by step name.   Value for each step is again a 
   *         map, keyed by manual input name.   And that value is
   *         yet another map with keys virtualPath, catalogKey,
   *         activity id and isOptional.
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object> getManualRunFilepaths(int run, String stepName)
    throws UnsupportedEncodingException, IOException, EtClientException {
    Integer iRun = new Integer(run);
    return getManualRunFilepaths(iRun.toString(), stepName);
  }

  /**
   * Return information about signature inputs for the specified run
   * @param run  (String)
   * @param stepName (String) Restrict to specified step.  null for all
   * @param activityStatus Array list of allowable activity status. If
   *        null treated as list with single entry "success".
   * @return map with scalar values for information about the run
   *         overall (traveler name, traveler version, hardware type,
   *         experimentSN, subsystem, run number, begin and end times,
   *         run status) and the key 'steps' whose value is a map
   *         keyed by step name.   Value for each step is again a 
   *         map, keyed by signerRequest. Value for a signerRequest is
   *         yet another map with keys activityId, inputPattern, 
   *         signerValue, signerCOmment and signatureTS.
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object>
    getManualRunSignatures(String run, String stepName,
                           ArrayList<String> activityStatus)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("run", run);
    args.put("function", "getManualRunSignatures");
    if (stepName != null) args.put("stepName", stepName);
    if (activityStatus != null) args.put("activityStatus", activityStatus);
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object> )
      consumeAck(results).get("results");
  }

  /**
   * Return information about signatures for the specified run
   * @param run  (int)
   * @param stepName (String) Restrict to specified step.  null for all
   * @return map with scalar values for information about the run
   *         overall (traveler name, traveler version, hardware type,
   *         experimentSN, subsystem, run number, begin and end times,
   *         run status) and the key 'steps' whose value is a map
   *         keyed by step name.   Value for each step is again a 
   *         map, keyed by signerRequest. Value for a signerRequest is
   *         yet another map with keys activityId, inputPattern, 
   *         signerValue, signerComment and signatureTS.
   * @param activityStatus Array list of allowable activity status. If
   *        null treated as list with single entry "success".
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object>
    getManualRunSignatures(int run, String stepName,
                           ArrayList<String> activityStatus)
    throws UnsupportedEncodingException, IOException, EtClientException {
    Integer iRun = new Integer(run);
    return getManualRunSignatures(iRun.toString(), stepName, activityStatus);
  }

  /**
   * Return information about manual inputs for specified traveler type,
   * step within the traveler and hardware type.   May further
   * qualify by model or experimentSN or list of hardware labels.  
   * @param  travelerName
   * @param  stepName
   * @param  hardwareType
   * @param  model (may be null)
   * @param  experimentSN (may be null)
   * @param  hardwareLabels (set of strings used to filter; may be null)
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object>
    getManualResultsStep(String travelerName, String stepName,
                         String hardwareType, String model,
                         String experimentSN, Set<String> hardwareLabels)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("function", "getManualResultsStep");
    args.put("travelerName", travelerName);
    args.put("stepName", stepName);
    args.put("hardwareType", hardwareType);
    if (model != null) {
      args.put("model", model);
    }
    if (experimentSN != null) {
      args.put("experimentSN", experimentSN);
    }
    if (hardwareLabels != null) {
      args.put("hardwareLabels", hardwareLabels);
    }
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object> ) consumeAck(results).get("results");
  }

  /* xxx */
  /**
   * Return information about manual filepath inputs for specified 
   * traveler type, step within the traveler and hardware type.  
   * May further qualify by model or experimentSN or list of hardware labels.  
   * @param  travelerName
   * @param  stepName
   * @param  hardwareType
   * @param  model (may be null)
   * @param  experimentSN (may be null)
   * @param  hardwareLabels (set of strings used to filter; may be null)
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object>
    getManualFilepathsStep(String travelerName, String stepName,
                           String hardwareType, String model,
                           String experimentSN, Set<String> hardwareLabels)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("function", "getManualFilepathsStep");
    args.put("travelerName", travelerName);
    args.put("stepName", stepName);
    args.put("hardwareType", hardwareType);
    if (model != null) {
      args.put("model", model);
    }
    if (experimentSN != null) {
      args.put("experimentSN", experimentSN);
    }
    if (hardwareLabels != null) {
      args.put("hardwareLabels", hardwareLabels);
    }
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object> ) consumeAck(results).get("results");
  }
  /**
   * Return information about signature inputs for specified traveler type,
   * step within the traveler and hardware type.  May further qualify
   * by model, experimentSN, or set of hardware labels  
   * @param  travelerName
   * @param  stepName
   * @param  hardwareType
   * @param  model (may be null)
   * @param  experimentSN (may be null)
   * @param  hardwareLabels (set of strings used to filter; may be null)
   * @param activityStatus Array list of allowable activity status. If
   *        null treated as list with single entry "success".
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public HashMap<String, Object>
    getManualSignaturesStep(String travelerName, String stepName,
                            String hardwareType, String model,
                            String experimentSN, Set<String> hardwareLabels,
                            ArrayList<String> activityStatus)
    throws UnsupportedEncodingException, IOException, EtClientException {
    HashMap<String, Object> args = new HashMap<String, Object> ();
    args.put("function", "getManualSignaturesStep");
    args.put("travelerName", travelerName);
    args.put("stepName", stepName);
    args.put("hardwareType", hardwareType);
    if (model != null) {
      args.put("model", model);
    }
    if (experimentSN != null) {
      args.put("experimentSN", experimentSN);
    }
    if (hardwareLabels != null) {
      args.put("hardwareLabels", hardwareLabels);
    }
    if (activityStatus != null) {
      args.put("activityStatus", activityStatus);
    }
    HashMap<String, Object> results =
      (HashMap<String, Object>) m_client.execute("getResults", args);
    return (HashMap<String, Object> ) consumeAck(results).get("results");
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
