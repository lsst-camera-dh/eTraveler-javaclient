package org.lsst.camera.etraveler.javaclient;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Set;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import com.rits.cloning.Cloner;

//import org.lsst.camera.etraveler.javaclient.getHarnessed.GetHarnessedData;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * This class provides a way to stash per-run data so it can be
 *  retrieved and optionally filtered without making a new request to
 *  Front-end or db.   Web applications wishing to hang on to data
 *  should create an EtClientDataServer object and stash it in a 
 *  session variable.
 * @author jrb
 */
public class EtClientDataServer {
  public static final int FRONTEND_PROD=1;
  public static final int FRONTEND_DEV=2;
  public static final int FRONTEND_LOCAL=3;

  // Only one experiment per data server
  private String m_experiment="LSST-CAMERA";
  private int m_frontend=FRONTEND_PROD;
  private Cloner m_cloner=null;
  private String m_appSuffix="";

  // Map datasource mode to services to fetch data for that mode
  private HashMap<String, EtClientServices> m_clientMap = null;
  
  // The String key is dataSourceMode (Dev, Prod, etc.); int key is run number
  // Innermost Object is meant to represent data from one run
  private HashMap<String, HashMap<Integer, HashMap<String, Object> > > m_allDataMap=null;
  /**
   * Assumes production front-end server, standard app name ("eTraveler")
   * @param experiment 
   */
  public EtClientDataServer(String experiment) {
    m_experiment = new String(experiment);
  }
  /**
   * Assumes standard app name ("eTraveler")
   * @param experiment
   * @param frontend 
   */
  public EtClientDataServer(String experiment, int frontend) {
    if ((frontend > 0 )  && (frontend < 4)) {
      m_frontend = frontend;
    }   // else use default
    m_experiment = experiment;
  }
/**
 * @param experiment
 * @param frontend
 * @param appSuffix Will be appended to "eTraveler"
 */
  public EtClientDataServer(String experiment, int frontend, String appSuffix) {
    m_appSuffix = appSuffix;
    if ((frontend > 0 )  && (frontend < 4)) {
      m_frontend = frontend;
    }   // else use default
    m_experiment = experiment;
  }  

  /**
   *  Fetches from "Prod" database
   * @param run
   * @return  Same as EtClientServices.getRunResults
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public Object fetchRun(String run)
    throws UnsupportedEncodingException, IOException, EtClientException {
    return fetchRun(run, "Prod");
  }
  /**
   * May specify alternate db
   * @param run
   * @param dataSourceMode
   * @return
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public Object fetchRun(String run, String dataSourceMode)
    throws UnsupportedEncodingException, IOException, EtClientException {
    EtClientServices client = null;
    HashMap<Integer, HashMap<String, Object> > allData=null;
    Integer runInt = formRunInt(run);
    if (m_clientMap == null) {
      m_clientMap = new HashMap<String, EtClientServices>();
      m_allDataMap = new
        HashMap<String, HashMap<Integer, HashMap<String, Object> > >();
    }
    if (m_cloner == null) {
      m_cloner = new Cloner();

      // m_cloner.setDumpClonedClasses(true);   // Temporary!!
    }
    if (!m_clientMap.containsKey(dataSourceMode)) {
      boolean prodServer=true;
      boolean localServer=false;
      switch (m_frontend) {
      case FRONTEND_PROD:
        break;
      case FRONTEND_DEV:
        prodServer=false;
        break;
      case FRONTEND_LOCAL:
        localServer=true;
        break;
      }
      client = new EtClientServices(dataSourceMode, m_experiment, prodServer,
                                    localServer, m_appSuffix);
      m_clientMap.put(dataSourceMode, client);
      allData = new HashMap<Integer, HashMap<String, Object>>();
      m_allDataMap.put(dataSourceMode, allData);
    } else {
      client = m_clientMap.get(dataSourceMode);
      allData = m_allDataMap.get(dataSourceMode);
    }

    
    HashMap<String, Object> savedResults = null;
    if (!allData.containsKey(runInt)) {
      savedResults = client.getRunResults(Integer.toString(runInt), null, null);
      allData.put(runInt, savedResults);
    } else {
      savedResults = allData.get(runInt);
    }
    HashMap<String, Object> results = m_cloner.deepClone(savedResults);

    return results;
  }
  /**
   * For schemas including an itemFilter keyword, return only data matching
   * specified value.  But all data for run is cached
   * @param run
   * @param itemFilters List of pairs specifying schema keyword and value,
   * e.g. ("amp", 3) or ("slot", "S02")
   * @return
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public Object fetchRun(String run,
                         ArrayList<ImmutablePair<String, Object>> itemFilters)
    throws UnsupportedEncodingException, IOException, EtClientException {
    return fetchRun(run, "Prod", null, null, itemFilters);
  }
  
  /**
   * Return only data satisfying constraints.  But all data for the run is
   * cached
   * @param run
   * @param dataSourceMode
   * @param step
   * @param schema
   * @param itemFilters
   * @return
   * @throws UnsupportedEncodingException
   * @throws IOException
   * @throws EtClientException 
   */
  public Object fetchRun(String run, String dataSourceMode, String step,
                         String schema,
                         ArrayList<ImmutablePair<String, Object> > itemFilters)
  throws UnsupportedEncodingException, IOException, EtClientException {

    HashMap<String, Object> results =
      (HashMap<String, Object>) fetchRun(run, dataSourceMode);

    // if step eliminate other steps
    if (step != null) removeSteps(results.get("steps"), step);

    // if schema eliiminate other schemas
    if (schema != null) removeSchemas(results.get("steps"), schema);
    
    // if itemFilter, prune
    if (itemFilters != null) {
      pruneRun((HashMap<String, Object>) results.get("steps"),
               itemFilters);
    }
    return results;
    
  }
  //private static HashMap<String, Object>
  private static void
    pruneRun(HashMap<String, Object> steps,
             ArrayList<ImmutablePair<String, Object>> filters) {
    if (filters == null) return;
    if (filters.size() == 0 ) return;
    for (Object oStep: steps.values() ) {    // for each step
      HashMap<String, Object> step = (HashMap<String, Object>) oStep;
      for (Object oSchema: step.values() ) {   //  for each schema
        ArrayList<HashMap<String, Object> > schema =
          (ArrayList<HashMap<String, Object> > ) oSchema;
        pruneSchema(schema, filters);
      }
      
    }
    //return results;
  }

  private static void removeSteps(Object stepsObj, String stepToKeep) {
    HashMap<String, Object> steps = (HashMap<String, Object>) stepsObj;

    Set<String> keys = new HashSet<String>((steps.keySet()));
    for (String stepName : keys ) {
      if (!stepName.equals(stepToKeep)) steps.remove(stepName);
    }
  }

  private static void removeSchemas(Object stepsObj, String schemaToKeep) {
    HashMap<String, Object> steps = (HashMap<String, Object>) stepsObj;
    Set<String> stepKeys = new HashSet<String>(steps.keySet());
    for (String stepName : stepKeys) {
      HashMap<String, Object> schemas =
        (HashMap<String, Object>) steps.get(stepName);
      Set<String> schemaKeys = new HashSet<String>(schemas.keySet());
      if (!schemaKeys.contains(schemaToKeep)) { // step is of no interest
        steps.remove(stepName);
      } else {
        for (String schemaName : schemaKeys) {
          if (!schemaName.equals(schemaToKeep)) schemas.remove(schemaName);
        }
      }
    }
  }

  private static void
    pruneSchema(ArrayList<HashMap<String,Object> > schemaData,
                ArrayList<ImmutablePair<String, Object> >filters) {

    for (ImmutablePair<String, Object> filter : filters) {
      String key = filter.getLeft();
      Object val = filter.getRight();

      HashMap<String, Object> instance0 = schemaData.get(0);
      if (!(instance0.containsKey(key))) continue;

      for (int i=(schemaData.size() - 1); i > 0; i--) {
        if (!(schemaData.get(i).get(key).equals(val)) ) {
          schemaData.remove(i);
        }
      }
    }
  }
  private static int formRunInt(String st) throws EtClientException {
    int theInt;
    try {
      theInt = Integer.parseInt(st);
      } catch (NumberFormatException e) {
      try {
        theInt = Integer.parseInt(st.substring(0, st.length() -1));
      }  catch (NumberFormatException e2) {
        throw new EtClientException("Supplied run value " + st +
                                        " is not valid");
      }
      if (theInt < 1) {
        throw new EtClientException("Supplied run value " + st +
                                    " is not valid");
      }
    }
    return theInt;
  }
}
    
