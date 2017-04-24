package org.lsst.camera.etraveler.javaclient;

import javax.servlet.jsp.JspContext;
import javax.servlet.jsp.PageContext;
import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.SimpleTagSupport;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

public class MakeDataServer extends SimpleTagSupport {
  private String m_experiment="LSST-CAMERA";
  private String m_frontend="prod";
  private String m_outputVariable=null;
  private int m_server=EtClientDataServer.FRONTEND_PROD;

  public void setOutputVariable(String arg) {m_outputVariable = arg;}
  public void setFrontend(String arg) {m_frontend = arg;}
  public void setExperiment(String arg) {m_experiment = arg;}

  public void doTag() throws JspException, IOException {
    if (m_frontend.equals("dev") ) m_server=EtClientDataServer.FRONTEND_DEV;
    if (m_frontend.equals("local") ) m_server=EtClientDataServer.FRONTEND_LOCAL;
    JspContext jspContext = getJspContext();

    EtClientDataServer dataServer =
      new EtClientDataServer(m_experiment, m_server);
    jspContext.setAttribute(m_outputVariable, dataServer,
                            PageContext.SESSION_SCOPE);
  }
  
}
