package org.lsst.camera.etraveler.javaclient;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Set;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import com.rits.cloning.Cloner;
import org.lsst.camera.etraveler.javaclient.utils.RunUtils;

//import org.lsst.camera.etraveler.javaclient.getHarnessed.GetHarnessedData;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * This class provides a way to cache per-run data so it can be
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
    Integer runInt = RunUtils.formRunInt(run);
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
    if (step != null) RunUtils.removeSteps(results.get("steps"), step);

    // if schema eliiminate other schemas
    if (schema != null) RunUtils.removeSchemas(results.get("steps"), schema);
    
    // if itemFilter, prune
    if (itemFilters != null) {
      RunUtils.pruneRun((HashMap<String, Object>) results.get("steps"),
                        itemFilters);
    }
    return results;
    
  }
}
    
