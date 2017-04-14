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
  
  // The String key is datasource mode (Dev, Prod, etc.); int key is run number
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


  Object fetchRun(int run)
    throws UnsupportedEncodingException, IOException, EtClientException {
    return fetchRun(run, "Prod");
  }
  Object fetchRun(int run, String datasource)
    throws UnsupportedEncodingException, IOException, EtClientException {
    return fetchRun(run, datasource, null);
  }
  Object fetchRun(int run, Pair itemFilter)
    throws UnsupportedEncodingException, IOException, EtClientException {
    return fetchRun(run, "Prod", itemFilter);
  }
      
  Object fetchRun(int run, String datasource, Pair itemFilter)
  throws UnsupportedEncodingException, IOException, EtClientException {
    EtClientServices client = null;
    HashMap<Integer, HashMap<String, Object> > allData=null;
    if (m_clientMap == null) {
      m_clientMap = new HashMap<String, EtClientServices>();
      m_allDataMap = new
        HashMap<String, HashMap<Integer, HashMap<String, Object> > >();
    }
    if (!m_clientMap.containsKey(datasource)) {
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
      client = new EtClientServices(datasource, m_experiment, prodServer,
                                    localServer);
      m_clientMap.put(datasource, client);
      allData = new HashMap<Integer, HashMap<String, Object>>();
      m_allDataMap.put(datasource, allData);
    } else {
      client = m_clientMap.get(datasource);
      allData = m_allDataMap.get(datasource);
    }

    
    HashMap<String, Object> savedResults = null;
    if (!allData.containsKey((Integer) run)) {
      savedResults = client.getRunResults(Integer.toString(run), null, null);
      allData.put(run, savedResults);
    } else {
      savedResults = allData.get(run);
    }
    HashMap<String, Object> results = (HashMap<String, Object>)
      savedResults.clone();

    // if itemFilter, prune
    return results;
    
  }
  private static HashMap<String, Object>
    pruneRun(HashMap<String, Object> results,
             ImmutablePair<String, Object> filter) {
    for (Object oStep: results.values() ) {    // for each step
      HashMap<String, Object> step = (HashMap<String, Object>) oStep;
      for (Object oSchema: step.values() ) {   //  for each schema
        ArrayList<HashMap<String, Object> > schema =
          (ArrayList<HashMap<String, Object> > ) oSchema;
        pruneSchema(schema, filter);
      }
    }
    return results;
  }
  private static void pruneSchema(ArrayList<HashMap<String,Object> > schemaData,
                                  ImmutablePair<String, Object> filter) {
    String key = filter.getLeft();
    Object val = filter.getRight();

    HashMap<String, Object> instance0 = schemaData.get(0);
    if (!(instance0.containsKey(key))) return;

    for (int i=(schemaData.size() - 1); i > 0; i--) {
      if (!(schemaData.get(i).get(key).equals(val)) ) {
        schemaData.remove(i);
      }
    }

  }
}
    
