package org.lsst.camera.etraveler.javaclient;

import javax.servlet.jsp.JspContext;
import javax.servlet.jsp.PageContext;
import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.SimpleTagSupport;
import javax.servlet.http.HttpServletRequest;
import java.sql.SQLException;
import java.io.IOException;
import java.util.Map;

/**
   The following inputs are needed
       Experiment name (not actually used yet)
       Db name
       Whether or not we want to talk to prod eT server
       Command to be executed
 */
public class EtClientTag extends SimpleTagSupport {
  private String m_dbname;
  private String m_experiment;
  private String m_command;
  private String m_activity;
  
  public void setDbname(String arg) {m_dbname = arg;}
  public void setCommand(String arg) {m_command = arg;}
  public void setExperiment(String arg) {m_experiment = arg;}
  public void setActivity(String arg) {m_activity = arg;}
  public void doTag() throws JspException,
  IOException {
    JspContext jspContext = getJspContext();
    HttpServletRequest
      request = (HttpServletRequest)((PageContext)jspContext).getRequest();


    //String dbname = request.getParameter("dbname");
    //String experiment = request.getParameter("experiment");
    boolean devServer = false;  // set to true if param is present

    //String command = request.getParameter("command");
    //String activity = request.getParameter("activity");
    if ((m_dbname == null) || (m_experiment==null) || 
        (m_command == null) || (m_activity==null)) {
      throw new JspException("Missing parameter(s)");
    }
        
    if (!m_command.equals("getRunInfo")) {
      throw new JspException("Unsupported command " + m_command);
    }
    int activityId = 0;
    try {
      activityId = Integer.parseInt(m_activity);
    } catch
        (NumberFormatException e) {
      throw new JspException("Non-integer value for activity parameter");
    }

    EtClientServices etClient =
      new EtClientServices(m_dbname, m_experiment, true);
    Map<String, Object> runInfo = null;
    try {
      runInfo = etClient.getRunInfo(activityId);
    } catch (EtClientException e) {
      throw new JspException(e.getMessage());
    }
    for (String k : runInfo.keySet()) {
      jspContext.setAttribute(k, runInfo.get(k));
    }
    
  }
}

  
