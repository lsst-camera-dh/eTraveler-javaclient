/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lsst.camera.etraveler.javaclient.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.lsst.camera.etraveler.javaclient.EtClientException;

/**
 * Static utilities associated with run data
 * @author jrb
 */
public class RunUtils {
  /**
   * Prune schema instances not satisfying filters 
   * @param steps Map with keys = step names
   * @param filters List of pairs (key, value) where key
   * may be a key for some schema.  Leave schema data alone if it doesn't
   * use the key
   */
   public static void
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
  }

  public static void removeSteps(Object stepsObj, String stepToKeep) {
    HashMap<String, Object> steps = (HashMap<String, Object>) stepsObj;

    Set<String> keys = new HashSet<String>((steps.keySet()));
    for (String stepName : keys ) {
      if (!stepName.equals(stepToKeep)) steps.remove(stepName);
    }
  }

  public static void removeSchemas(Object stepsObj, String schemaToKeep) {
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
  /**
   * Given string of proper form, return corresponding run number.
   * String must be string rep. of positive integer optionally followed
   * by one non-numeric character
   * @param st   input string
   * @return     resulting integer
   * @throws EtClientException 
   */
  public static int formRunInt(String st) throws EtClientException {
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
