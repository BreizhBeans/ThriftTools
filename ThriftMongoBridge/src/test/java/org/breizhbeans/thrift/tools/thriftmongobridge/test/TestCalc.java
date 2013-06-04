package org.breizhbeans.thrift.tools.thriftmongobridge.test;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: c2895
 * Date: 04/06/13
 * Time: 11:39
 * To change this template use File | Settings | File Templates.
 */
public class TestCalc {


    @Test
    public void minus() {
       Calc calc = new Calc();

        int result = calc.minus(5,1);

        Assert.assertEquals(4, result);
    }
}
