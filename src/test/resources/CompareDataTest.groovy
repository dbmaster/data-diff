/*
 *  File Version:  $Id: CompareDataTest.groovy 145 2013-05-22 18:10:44Z schristin $
 */

package org.dbmaster.tools.db_compare_data;

import com.branegy.testing.dbmaster.tools.BaseToolTestNGCase;

class CompareDataTest extends BaseToolTestNGCase {

    @Test
    public void test(){
        Object out = 123;// tools.execute("DB-COMPARE-DATA",[name:"Gromit", likes:"cheese", id:1234],false);
        System.out.println(out);
    }

}
