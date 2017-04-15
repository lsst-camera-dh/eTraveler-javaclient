package org.lsst.camera.etraveler.javaclient;

/**
   This class provides a way to stash per-run data so it can be
   retrieved and optionally filtered without making a new request to
   Front-end or db.   Web applications wishing to hang on to data
   should create an EtClientDataServer object and stash it in a 
   session variable.
 */

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Set;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

//import org.lsst.camera.etraveler.javaclient.getHarnessed.GetHarnessedData;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

class EtClientDataServer {
  public static final int FRONTEND_PROD=1;
  public static final int FRONTEND_DEV=2;
  public static final int FRONTEND_LOCAL=3;

  // Only one experiment per data server
  private String m_experiment="LSST-CAMERA";
  private int m_frontend=FRONTEND_PROD;

  // Map datasource mode to services to fetch data for that mode
  private HashMap<String, EtClientServices> m_clientMap = null;
  
  // The String key is dataSourceMode (Dev, Prod, etc.); int key is run number
  // Innermost Object is meant to represent data from one run
  private HashMap<String, HashMap<Integer, HashMap<String, Object> > > m_allDataMap=null;
  EtClientDataServer(String experiment) {
    m_experiment = new String(experiment);
  }
  EtClientDataServer(String experiment, int frontend) {
    if ((frontend > 0 )  && (frontend < 4)) {
      m_frontend = frontend;
    }   // else use default
    m_experiment = experiment;
  }

  

  public Object fetchRun(String run)
    throws UnsupportedEncodingException, IOException, EtClientException {
    return fetchRun(run, "Prod");
  }
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
                                    localServer);
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
    HashMap<String, Object> results = (HashMap<String, Object>)
      savedResults.clone();

    return results;
  }
  public Object fetchRun(String run,
                         ArrayList<ImmutablePair<String, Object>> itemFilters)
    throws UnsupportedEncodingException, IOException, EtClientException {
    return fetchRun(run, "Prod", null, null, itemFilters);
  }
      
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
      return pruneRun(results, itemFilters);
    }
    return results;
    
  }
  private static HashMap<String, Object>
    pruneRun(HashMap<String, Object> results,
             ArrayList<ImmutablePair<String, Object>> filters) {
    if (filters == null) return null;
    if (filters.size() == 0 ) return null;
    for (Object oStep: results.values() ) {    // for each step
      HashMap<String, Object> step = (HashMap<String, Object>) oStep;
      for (Object oSchema: step.values() ) {   //  for each schema
        ArrayList<HashMap<String, Object> > schema =
          (ArrayList<HashMap<String, Object> > ) oSchema;
        pruneSchema(schema, filters);
      }
      
    }
    return results;
  }

  private static void removeSteps(Object stepsObj, String stepToKeep) {
    HashMap<String, Object> steps = (HashMap<String, Object>) stepsObj;

    Set<String> keys = steps.keySet();
    for (String stepName : keys ) {
      if (!stepName.equals(stepToKeep)) steps.remove(stepName);
    }
  }

  private static void removeSchemas(Object stepsObj, String schemaToKeep) {
    HashMap<String, Object> steps = (HashMap<String, Object>) stepsObj;
    Set<String> stepKeys = steps.keySet();
    for (String stepName : stepKeys) {
      HashMap<String, Object> schemas =
        (HashMap<String, Object>) steps.get(stepName);
      Set<String> schemaKeys = schemas.keySet();
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
    