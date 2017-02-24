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
