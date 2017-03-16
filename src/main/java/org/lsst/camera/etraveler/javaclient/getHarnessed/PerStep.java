package org.lsst.camera.etraveler.javaclient.getHarnessed;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;


/**
   Store data associated with one step as HashMap.  Keys are schema names.
   For each schema used by the step value is an ArrayList of maps,
   one for each schema instance.
*/
public class PerStep implements Map<String, PerSchema> {
  private HashMap<String, PerSchema> m_data = null;

  public PerStep() {
    m_data = new HashMap<String, PerSchema> ();
  }

  public PerSchema addSchema(String schemaName) {
    m_data.put(schemaName, new PerSchema());
    return m_data.get(schemaName);
  }

  public PerSchema  findSchema(String schemaName) {
    return m_data.get(schemaName);
  }

  public PerSchema  findOrAddSchema(String schemaName) {
    if (findSchema(schemaName) != null) return findSchema(schemaName);
    return addSchema(schemaName);
  }

  public int prune(Pair<String, Object> filter, int dtype)
  throws GetHarnessedException {
    
    for (String schemaName : m_data.keySet() ) {
      int retType = m_data.get(schemaName).prune(filter, dtype);
      //      if (dtype == PerSchema.DT_ABSENT) return dtype;
    }
    return dtype;
  }

  /* Implementation of Map interface */
  public void clear() {
    m_data.clear();
  }

  public int size() {
    return m_data.size();
  }
  
  public boolean containsKey(Object key) {
    return m_data.containsKey(key);
  }

  public boolean containsValue(Object value) {
    return m_data.containsValue(value);
  }

  public Set<Map.Entry<String, PerSchema>> entrySet() {
    return m_data.entrySet();
  }

  public boolean equals(Object o) {
    return m_data.equals(o);
  }

  public PerSchema get(Object key) {
    return m_data.get(key);
  }

  public int hashCode() {
    return m_data.hashCode();
  }

  public boolean isEmpty() {
    return m_data.isEmpty();
  }

  public Set<String> keySet() {
    return m_data.keySet();
  }

  public PerSchema put(String key, PerSchema value) {
    return m_data.put(key, value);
  }

  public void putAll(Map<? extends String, ? extends PerSchema> m) {
    m_data.putAll(m);
  }

  public PerSchema remove(Object key) {
    return m_data.remove(key);
  }

  public Collection<PerSchema> values() {
    return m_data.values();
  }
}
