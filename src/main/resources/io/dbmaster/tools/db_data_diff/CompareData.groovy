package io.dbmaster.tools.db_data_diff

import java.util.*

import groovy.sql.Sql
import com.branegy.dbmaster.database.api.ModelService
import com.branegy.dbmaster.model.*
import com.branegy.service.connection.api.ConnectionService
import com.branegy.dbmaster.connection.ConnectionProvider
import org.apache.commons.io.IOUtils
import java.io.PrintWriter
import java.io.StringWriter
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.util.ArrayList
import java.util.Arrays
import java.util.LinkedHashSet
import java.util.List
import java.util.Set
import org.slf4j.Logger


public class CompareData {
    protected final StringWriter stringWriter = new StringWriter();
    protected final PrintWriter printWriter = new PrintWriter(stringWriter);
    protected static final String COLOR_NEW     = "rgb(196, 215, 155)";
    protected static final String COLOR_DELETED = "rgb(249, 154, 156)";
    protected static final String COLOR_UPDATED = "rgb(255, 252, 185)";
    protected static final String COLOR_CHANGED = "rgb(255, 255, 128)";


    protected Logger logger
    protected List<String> pkColumns
    protected List<String> nonKeyColumns
    protected Integer[] compareMapping

    public compareResultSets(ResultSet rs1, ResultSet rs2, String compareKey, logger) throws SQLException {
        ResultSetMetaData rsmd1 = rs1.getMetaData()
        ResultSetMetaData rsmd2 = rs2.getMetaData()
        this.logger = logger

        if (rsmd1.getColumnCount() != rsmd2.getColumnCount()) {
            throw new IllegalStateException("Column count is not equal, " + rsmd1.getColumnCount() + "/"
                    + rsmd2.getColumnCount())
        }

        List<String> columnList1 = new ArrayList<String>();
        for (int i = 1; i <= rsmd1.getColumnCount(); ++i) {
            columnList1.add(rsmd1.getColumnName(i))
        }
        List<String> columnList2 = new ArrayList<String>();
        for (int i = 1; i <= rsmd2.getColumnCount(); ++i) {
            columnList2.add(rsmd2.getColumnName(i))
        }

        pkColumns = compareKey.split(",").collect { it.trim() }

        // TODO check if any column appears twice in the list

        int[] pk1 = new int[pkColumns.size()]
        int[] pk2 = new int[pkColumns.size()]

        int i = 0;
        for (String col : pkColumns) {
            int index
            index = columnList1.indexOf(col)
            if (index == -1) {
                throw new IllegalStateException("Column ${col} not found in source data set")
            }
            columnList1.set(index, null)
            pk1[i] = index + 1;

            index = columnList2.indexOf(col)
            if (index == -1) {
                throw new IllegalStateException("Column ${col} not found in target data set")
            }
            columnList2.set(index, null);
            pk2[i] = index + 1;

            i++;
        }

        compareMapping = new Integer[rsmd1.getColumnCount()];
        nonKeyColumns = new ArrayList<String>();
        for (i = 0; i < columnList1.size(); ++i) {
            String col = columnList1.get(i);
            if (col != null && !pkColumns.contains(col)) {
                int index = columnList2.indexOf(col);
                if (index == -1) {
                    throw new IllegalStateException("Column " + col + " not found in target data set");
                }
                compareMapping[i] = index + 1;
                nonKeyColumns.add(col);
            }
        }

        // process data
        Object[] pkValue1 = null;
        Object[] pkValue2 = null;

        pkValue1 = nextAndReturnPk(rs1, pk1, pkValue1, "source")
        pkValue2 = nextAndReturnPk(rs2, pk2, pkValue2, "target")

        // printWriter.print("Key Columns = " + pkColumns.join("::::"))
        // printWriter.print("Non Key Columns = " + nonKeyColumns.join("::::"))

        onStart(pkColumns, nonKeyColumns);
        while (true) {
            logger.info("Comparing ${pkValue1} with ${pkValue2}")
            if (pkValue1 == null && pkValue2 == null) {
                logger.info("Done")
                break;
            } else if (pkValue1 == null) {
                // mark all v2 as new
                onNewRecord(pkValue2, read2(rs2, compareMapping));
                pkValue2 = nextAndReturnPk(rs2, pk2, pkValue2,"target");
            } else if (pkValue2 == null) {
                // mark all v1 as deleted
                onDeletedRecords(pkValue1, read1(rs1, compareMapping));
                pkValue1 = nextAndReturnPk(rs1, pk1, pkValue1,"source");
            } else {
                int compare = comparePk(pkValue1, pkValue2);
                if (compare == 0) {
                    // compare
                    def v1 = read1(rs1, compareMapping)
                    def v2 = read2(rs2, compareMapping)
                    if (!v1.equals(v2)) {
                        onChangedRecords(pkValue1, v1, v2)
                    } else {
                        onEqualRecords(pkValue1, v1, v2)
                    }
                    pkValue1 = nextAndReturnPk(rs1, pk1, pkValue1, "source");
                    pkValue2 = nextAndReturnPk(rs2, pk2, pkValue2, "target");

                    //Object[] newPkValue2 = nextAndReturnPk(rs2, pk2, pkValue2, "target");
                    //if (newPkValue2!=null && comparePk(pkValue2, newPkValue2) != 0) {
                    //    pkValue1 = nextAndReturnPk(rs1, pk1, pkValue1, "source");
                   // }
                    // pkValue2 = newPkValue2;
                } else if (compare < 0) {
                    // source less then target
                    // mark v1 removed
                    onDeletedRecords(pkValue1, read1(rs1, compareMapping));
                    pkValue1 = nextAndReturnPk(rs1, pk1, pkValue1,"source");
                } else {
                    // source more then target
                    // mark v2 added
                    onNewRecord(pkValue2, read2(rs2, compareMapping));
                    pkValue2 = nextAndReturnPk(rs2, pk2, pkValue2,"target");
                }
            }
        }
        onStop();
    }

    protected Object getColumnValue(String columnName, Object[] key, List<Object> values) {
        def index = pkColumns.indexOf(columnName)
        if (index>=0) {
            return key[index]
        }
        index = nonKeyColumns.indexOf(columnName)
        if (index>=0) {
            return values.get(index)
        }
        throw new RuntimeException("Column ${columnName} not found")
    }

    private Object[] nextAndReturnPk(ResultSet rs, int[] pk, Object[] prev, String set) throws SQLException {
        if (!rs.next()) {
            return null;
        }
        int j = 0;
        Object[] pkValue = new Object[pk.length];
        for (int i : pk) {
            pkValue[j++] = rs.getObject(i);
        }

        if (prev!=null){
            int compareResult = comparePk(prev, pkValue);
            if (compareResult == 0) {
                logger.warn("Non uninue primary key for " + Arrays.toString(pkValue) +
                                               " in " + set +" dataset" );
            } else if (compareResult > 0) {
                throw new IllegalStateException("Primary key should increment, but the following "+
                               "sequence detected " + Arrays.toString(prev) + "/" + Arrays.toString(pkValue)+
                               " in " + set +" dataset");
            }
        }
        return pkValue;
    }

    @SuppressWarnings("unchecked")
    private int comparePk(Object[] pk1, Object[] pk2) {
        for (int i = 0; i < pk1.length; ++i) {
            Comparable<Object> v1 = (Comparable<Object>) pk1[i];
            Comparable<Object> v2 = (Comparable<Object>) pk2[i];
            int r = v1.compareTo(v2);
            if (r != 0) {
                return r;
            }
        }
        return 0;
    }

    private List<Object> read2(ResultSet rs2, Integer[] compareMapping) throws SQLException {
        List<Object> v2 = new ArrayList<Object>();
        for (int j = 0; j < compareMapping.length; ++j) {
            if (compareMapping[j] != null) {
                v2.add(rs2.getObject(compareMapping[j]));
            }
        }
        return v2;
    }

    private List<Object> read1(ResultSet rs1, Integer[] compareMapping) throws SQLException {
        List<Object> v1 = new ArrayList<Object>();
        for (int j = 0; j < compareMapping.length; ++j) {
            if (compareMapping[j] != null) {
                v1.add(rs1.getObject(j + 1));
            }
        }
        return v1;
    }

    protected void onNewRecord(Object[] pk, List<Object> values) {
        printWriter.print("<tr style=\"background-color:${COLOR_NEW};\">");
        for (Object c:pk){
            printWriter.println("<td>"+c+"</td>");
        }
        for (Object c:values){
            printWriter.println("<td>"+c+"</td>");
        }
        printWriter.print("</tr>");
    }

    protected void onDeletedRecords(Object[] pk, List<Object> values) {
        printWriter.print("<tr style=\"background-color:"+COLOR_DELETED+";\">");
        for (Object c:pk){
            printWriter.println("<td>"+c+"</td>");
        }
        for (Object c:values){
            printWriter.println("<td>"+c+"</td>");
        }
        printWriter.print("</tr>");
    }

    protected void onChangedRecords(Object[] pk, List<Object> v1, List<Object> v2) {
        printWriter.print("<tr style=\"background-color:"+COLOR_UPDATED+";\">")

        for (Object c : pk) {
            printWriter.println("<td>"+c+"</td>");
        }

        for (int i = 0; i < v1.size(); ++i) {
            Object o1 = v1.get(i);
            Object o2 = v2.get(i);
            if (equalWithNull(o1, o2)) {
                printWriter.println("<td>"+o1+"</td>");
            } else {
                printWriter.println("<td style=\"background-color:"+COLOR_CHANGED+"\">"+o1+" -&gt; "+o2+"</td>");
            }
        }
    }

    protected void onEqualRecords(Object[] pk, List<Object> v1, List<Object> v2) {
        printWriter.print("<tr>");
        for (Object c:pk){
            printWriter.println("<td>"+c+"</td>");
        }
        for (Object c:v1){
            printWriter.println("<td>"+c+"</td>");
        }
        printWriter.print("</tr>");
    }

    protected void onStart(List<String> keyColumns, List<String> nonKeyColumns){
        printWriter.println("<table cellspacing=\"0\" class=\"simple-table\" border=\"1\"><tr>");
        printWriter.println(keyColumns.collect {"<td>${it}</td>"}.join());
        printWriter.println(nonKeyColumns.collect {"<td>${it}</td>"}.join());
        printWriter.println("</tr>");
    }

    protected void onStop(){
        printWriter.println("</table>");
    }

    public static boolean equalWithNull(Object obj1, Object obj2) {
        if (obj1 == obj2)
            return true;
        if (obj1 == null) {
            return false;
        }
        return obj1.equals(obj2);
    }

    @Override
    public String toString() {
        return stringWriter.toString();
    }

}
