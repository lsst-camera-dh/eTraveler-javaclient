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
  public void doTag() throws JspException,
  IOException {
    JspContext jspContext = getJspContext();
    HttpServletRequest
      request = (HttpServletRequest)((PageContext)jspContext).getRequest();


    String dbname = request.getParameter("dbname");
    String experiment = request.getParameter("experiment");
    boolean devServer = false;  // set to true if param is present

    String command = request.getParameter("command");
    String activity = request.getParameter("activity");
    if ((dbname == null) || (experiment==null) || 
        (command == null) || (activity==null)) {
      throw new JspException("Missing parameter(s)");
    }
        
    if (!command.equals("getRunInfo")) {
      throw new JspException("Unsupported command " + command);
    }
    int activityId = 0;
    try {
      activityId = Integer.parseInt(activity);
    } catch
        (NumberFormatException e) {
      throw new JspException("Non-integer value for activity parameter");
    }

    EtClientServices etClient =
      new EtClientServices(dbname, experiment, true);
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

  
