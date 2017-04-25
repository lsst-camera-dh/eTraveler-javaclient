package org.lsst.camera.etraveler.javaclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.entity.SerializableEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.impl.client.CloseableHttpClient;

import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;


/**
 * Encapsulates information needed to post to an eTraveler Front-end server.
 * Users typically do not instantiate directly.  See instead
 * @see org.lsst.camera.etraveler.javaclient.EtClientServices
 * and @see org.lsst.camera.etraveler.javaclient.EtClientDataServer
 * @author jrb
 */
public class EtClient {
  private String m_db="Prod";
  private String m_exp="LSST-CAMERA";
  private boolean m_prodServer=true;
  private boolean m_localServer=false;
  private String m_appSuffix="";
  private CloseableHttpClient m_httpclient = null;
  
  private static final String s_prodURL = "http://lsst-camera.slac.stanford.edu/eTraveler";
  private static final String s_devURL = "http://lsst-camera-dev.slac.stanford.edu/eTraveler";

  private static final String s_localURL = "http://localhost:8084/eTraveler";

  private class MyResponseHandler implements ResponseHandler< Map<String, Object > > {
    public Map<String, Object> handleResponse(final HttpResponse response) throws
      ClientProtocolException, IOException {
      //System.out.println("Inside handleResponse\n");
      int status = response.getStatusLine().getStatusCode();
      //System.out.println("Returned response was ");
      //System.out.println(status);
      if (status >= 200 && status < 305) {
        HttpEntity entity = response.getEntity();
        if (entity == null) return null;

        String stringData = EntityUtils.toString(entity);
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> results =
          (Map<String, Object>) mapper.readValue(stringData, Object.class);
        return results;
      } else {
        throw new
          ClientProtocolException("Unexpected http response status: " + status);
      }
    }
  }
  
  /**
   * 
   * @param db      Database to access ("Prod", "Dev", ..)
   * @param exp     Experiment (e.g. "LSST-CAM")
   */
  public EtClient(String db, String exp) {
    if (db != null) m_db = db;
    if (exp != null) m_exp = exp;
    createClient();
  }

  /**
   * 
   * @param db      Database to access ("Prod", "Dev",..)
   * @param exp     Experiment (e.g., "LSST-CAM")
   * @param appSuffix Used to specify alternate app name
   */
  public EtClient(String db, String exp, String appSuffix) {
    if (appSuffix != null) m_appSuffix = appSuffix;
    if (db != null) m_db = db;
    if (exp != null) m_exp = exp;
    createClient();
  }

  /**
   * By default production server is used
   * @param isProd 
   */
  public void setProdServer(boolean isProd) {
    m_prodServer = isProd;
  }
  /**
   * By default local server is NOT used
   */
  public void useLocalServer() {
    m_prodServer = false;
    m_localServer = true;
  }
  private void createClient() {
    m_httpclient =
      HttpClientBuilder.create().setRedirectStrategy(new LaxRedirectStrategy()).build();
  }
  
  private String formURL(String command)  {
    String url = s_prodURL;
    if (!m_prodServer) url = s_devURL;
    if (m_localServer) url = s_localURL;
    url += (m_appSuffix + "/" + m_db + "/Results/" + command);
    return url;
  }
  
  /**
   * Send command to a front-end server; return results. This routine is
   * invoked by methods in @see org.lsst.camera.etraveler.javaclient.EtClientServices
   * @param command  
   * @param args
   * @return
   * @throws JsonProcessingException
   * @throws UnsupportedEncodingException
   * @throws EtClientException
   * @throws IOException 
   */
  public Map<String, Object> execute(String command,
                                     HashMap<String, Object> args)
  throws JsonProcessingException, UnsupportedEncodingException,
         EtClientException, IOException {
    if (m_httpclient == null) createClient();

    System.out.println("Using URL " + formURL(command));
    HttpPost httppost = new HttpPost(formURL(command));
    String payload = new ObjectMapper().writeValueAsString(args);
    List<NameValuePair> params = new ArrayList<NameValuePair>(1);
    params.add(new BasicNameValuePair("jsonObject", payload));

    httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));

    MyResponseHandler hand = new MyResponseHandler();
    return m_httpclient.execute(httppost, hand);
  }

  public void close() throws IOException {
    if (m_httpclient != null) {
      m_httpclient.close();
      m_httpclient = null;
    }
  }
  
}
