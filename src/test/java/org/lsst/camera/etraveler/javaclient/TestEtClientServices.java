package org.lsst.camera.etraveler.javaclient;

import org.lsst.camera.etraveler.javaclient.EtClientServices;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.*;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;


import java.io.UnsupportedEncodingException;

public class TestEtClientServices {

  private HashMap<String, Object> m_params;


  @Before
  public void setup() {

  }
  
  @Test
  public void testGetRunInfo() throws UnsupportedEncodingException,
                                      EtClientException, IOException {
    System.out.println("Running testGetRunInfo test");

    EtClientServices myService = new EtClientServices();

    try {
      Map<String, Object> results = myService.getRunInfo(200);

      assertNotNull(results);
      for (String k: results.keySet() ) {
        Object v = results.get(k);
        if (v == null) {
          System.out.println("Key '" + k + "' has value null");
        } else {
          System.out.println("Key '" + k + "' has value: " + v.toString());
        }
      }
    } catch (Exception ex) {
      System.out.println("post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    }
    finally {
      myService.close();
    }
  }

  @Test
  public void testGetManufacturerId() throws UnsupportedEncodingException,
                                      EtClientException, IOException {
    System.out.println("Running testGetManufacturerId test");

    EtClientServices myService = new EtClientServices("Dev", null, true);

    try {
      Map<String, Object> results =
        myService.getManufacturerId("E2V-CCD250-179", "e2v-CCD");

      assertNotNull(results);
      for (String k: results.keySet() ) {
        Object v = results.get(k);
        if (v == null) {
          System.out.println("Key '" + k + "' has value null");
        } else {
          System.out.println("Key '" + k + "' has value: " + v.toString());
        }
      }
    } catch (Exception ex) {
      System.out.println("post failed with message " + ex.getMessage());
      throw new EtClientException(ex.getMessage());
    }
    finally {
      myService.close();
    }
  }

}
