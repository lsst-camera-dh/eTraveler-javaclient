package org.lsst.camera.etraveler.javaclient;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

public class TestRunner {
  public static void main(String[] args) {
    Result result = JUnitCore.runClasses(TestEtClientServices.class);
  }
}
