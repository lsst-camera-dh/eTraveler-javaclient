package org.lsst.camera.etraveler.javaclient;

import javax.servlet.jsp.JspContext;
import javax.servlet.jsp.PageContext;
import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.SimpleTagSupport;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class FetchRunData extends SimpleTagSupport {
  private EtClientDataServer m_server=null;  
  private String m_run=null;
  private String m_dataSourceMode="Prod";
  private String m_step=null;  // optionally select just one step
  private String m_schema=null;  // optionally select just one schema
  private String m_outputVariable=null;
  //  Filtering by key-value.  Pair?  list of pairs?
  ArrayList<ImmutablePair <String, Object>> m_itemFilters=null;

  public void setServer(EtClientDataServer arg) {m_server=arg;}
  public void setRun(String arg) {m_run=arg;}
  public void setStep(String arg) {m_step=arg;}
  public void setSchema(String arg) {m_schema=arg;}
  public void setDataSourceMode(String arg) {m_dataSourceMode=arg;}
  public void setOutputVariable(String arg) {m_outputVariable = arg;}

  public void setItemFilters(ArrayList<Object> arg) {
    m_itemFilters= new ArrayList<ImmutablePair <String, Object> >();
    for (Object o : arg) {
      ImmutablePair<String, Object> p = (ImmutablePair<String, Object>) o;
      m_itemFilters.add(p);
    }
  }

  public void doTag() throws JspException, IOException {
    JspContext jspContext = getJspContext();


    Object results = null;
    try {
      results = m_server.fetchRun(m_run, m_dataSourceMode, m_step,
                                  m_schema, m_itemFilters);
    } catch (EtClientException etExcept) {
      throw new JspException("EtClientException: " + etExcept.getMessage());
    }
    jspContext.setAttribute(m_outputVariable, results);
    
  }

}
