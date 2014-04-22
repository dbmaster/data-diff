package io.dbmaster.tools.diff;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.slf4j.Logger;

public abstract class CompareData {
    private final Logger logger;
    protected final PrintWriter printWriter;

    private boolean pkCompare1;
    private boolean pkCompare2;
    private int limit;

    public static void executeAsync(Connection c1, Connection c2, String sql1, String sql2, String compareKey,Long limit, Logger logger,
            PrintWriter printWriter)
                throws SQLException, InterruptedException{
            final CyclicBarrier barrier = new CyclicBarrier(3);

            AsynStatement connector1 = new AsynStatement(barrier, c1, sql1, logger ,"source");
            connector1.start();
            AsynStatement connector2 = new AsynStatement(barrier, c2, sql2, logger, "target");
            connector2.start();

            try{
                barrier.await();
            } catch (BrokenBarrierException e){
            }

            if (connector1.isException()!=null){
                throw new IllegalStateException(connector1.isException());
            }
            if (connector2.isException()!=null){
                throw new IllegalStateException(connector2.isException());
            }

            new DecoratedCompareData(connector1.getRs(), connector2.getRs(), compareKey, limit, logger, printWriter);
    }

    public static String executeAsync(Connection c1, Connection c2, String sql1, String sql2, String compareKey, Long limit, Logger logger)
            throws SQLException, InterruptedException{
        StringWriter writer = new StringWriter(1024*1024);
        PrintWriter printWriter = new PrintWriter(writer);
        executeAsync(c1, c2, sql1, sql2, compareKey, limit, logger, printWriter);
        printWriter.flush();
        return writer.toString();
    }

    public CompareData(ResultSet rs1, ResultSet rs2, String compareKey, Logger logger, boolean caseInsensitive, PrintWriter printWriter, Long limit)
            throws SQLException {
        this.logger = logger;
        this.printWriter = printWriter;
        this.limit = limit == null || limit == 0? 0: limit.intValue();

        List<ColumnInfo> columnList1 = getColumnInfoList(rs1.getMetaData());
        List<ColumnInfo> columnList2 = getColumnInfoList(rs2.getMetaData());

        String msg = "<p>Source columns: "+columnList1.toString()+"</p>"+ "<p>Target columns: "+columnList2.toString()+"</p>";

        Set<String> pkSet = new LinkedHashSet<String>(Arrays.asList(compareKey.trim().split("\\s*,\\s*")));

        List<ColumnMapperInfo> pkList = fillPkInfo(columnList1, columnList2, msg, pkSet, caseInsensitive);

        // collect all names
        Set<String> allColumns = new LinkedHashSet<String>();
        for (ColumnInfo ci:columnList1){
            allColumns.add(caseInsensitive?ci.name.toLowerCase():ci.name);
        }
        for (ColumnInfo ci:columnList2){
            allColumns.add(caseInsensitive?ci.name.toLowerCase():ci.name);
        }

        List<ColumnMapperInfo> columnList = new ArrayList<ColumnMapperInfo>(allColumns.size());
        for (String column:allColumns){
            ColumnInfo ci1 = exclude(columnList1, column, caseInsensitive);
            ColumnInfo ci2 = exclude(columnList2, column, caseInsensitive);
            ColumnMapperInfo cmi = new ColumnMapperInfo();
            cmi.name = ci1!=null?ci1.name:ci2.name;
            cmi.columnA = ci1;
            cmi.columnB = ci2;
            columnList.add(cmi);
        }

        ColumnMapperInfo[] pk = pkList.toArray(new ColumnMapperInfo[pkList.size()]);
        ColumnMapperInfo[] column = columnList.toArray(new ColumnMapperInfo[columnList.size()]);
        onStart(pk, column);
        compareData(pk, column, rs1, rs2);
        onStop();
    }

    private List<ColumnInfo> getColumnInfoList(ResultSetMetaData rsmd) throws SQLException{
        List<ColumnInfo> columnInfoList = new ArrayList<ColumnInfo>(rsmd.getColumnCount()-1);
        for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
            ColumnInfo info = new ColumnInfo();
            info.name = rsmd.getColumnName(i);
            info.type = rsmd.getColumnType(i);
            info.typeName = rsmd.getColumnTypeName(i);
            info.typeClass = rsmd.getColumnClassName(i);
            info.rsIndex = i;
            columnInfoList.add(info);
        }
        return columnInfoList;
    }

    private ColumnInfo exclude(List<ColumnInfo> list, String name, boolean caseInsensitive){
        Iterator<ColumnInfo> it = list.iterator();
        ColumnInfo info = null;
        while (it.hasNext()){
            info = it.next();
            if ((caseInsensitive && info.name.equalsIgnoreCase(name)) || info.name.equals(name)){
                it.remove();
                return info;
            }
        }
        return null;
    }

    private void compareData(ColumnMapperInfo[] pkList, ColumnMapperInfo[] columnList,
            ResultSet rs1, ResultSet rs2) throws SQLException {
        int count1 = 0;
        int count2 = 0;
        boolean pk1 = nextPk1(rs1, pkList);
        boolean pk2 = nextPk2(rs2, pkList);
        while (true) {
            if (pk1 && pk2) {
                int compare = comparePk(pkList);
                if (compare == 0) {
                    // compare
                    read1(rs1, columnList);
                    read2(rs2, columnList);
                    markAsChanged(pkList,columnList);
                    pk1 = nextPk1(rs1, pkList);
                    pk2 = nextPk2(rs2, pkList);
                    count1++;
                    count2++;
                } else if (compare < 0) {
                    // source less then target
                    // mark v1 removed
                    read1(rs1, columnList);
                    markAsDeleted(pkList, columnList);
                    pk1 = nextPk1(rs1, pkList);
                    count1++;
                } else {
                    // source more then target
                    // mark v2 added
                    read2(rs2, columnList);
                    markAsNew(pkList, columnList);
                    pk2 = nextPk2(rs2, pkList);
                    count2++;
                }
            } else if (pk1) {
                // mark all v1 as deleted
                read1(rs1, columnList);
                markAsDeleted(pkList,columnList);
                pk1 = nextPk1(rs1, pkList);
                count1++;
            } else if (pk2) {
                // mark all v2 as new
                read2(rs2, columnList);
                markAsNew(pkList, columnList);
                pk2 = nextPk2(rs2, pkList);
                count2++;
            } else {
                break;
            }
            if (count1 == limit){
                if (pk1){
                    logger.info("Limit {} was exceeded for source", limit);
                    break;
                }
            } 
            if (count2 == limit){
                if (pk2){
                    logger.info("Limit {} was exceeded for target", limit);
                    break;
                }
            }
        }
    }

    private List<ColumnMapperInfo> fillPkInfo(List<ColumnInfo> columnList1,
            List<ColumnInfo> columnList2, String msg, Set<String> pkSet, boolean caseInsensitive) {
        List<ColumnMapperInfo> pkList = new ArrayList<ColumnMapperInfo>();
        for (String col : pkSet) {
            ColumnInfo ci;
            ci = exclude(columnList1,col, caseInsensitive);
            if (ci == null) {
                throw new IllegalStateException("Compare pk column '"+col+"' is not found in the source. "+msg);
            }
            ColumnMapperInfo info = new ColumnMapperInfo();

            info.name = col;
            info.columnA = ci;

            ci = exclude(columnList2,col, caseInsensitive);
            if (ci == null) {
                throw new IllegalStateException("Compare pk column '"+col+"' is not found in the target. "+msg);
            }

            info.columnB = ci;
            if (info.columnA.type!=info.columnB.type){
                logger.warn("PK colum "+col+" have different type "+info.columnA.typeClass+"/"+info.columnB.typeClass);
            }
            pkList.add(info);
        }
        return pkList;
    }

    private boolean nextPk1(ResultSet rs1, ColumnMapperInfo[] pkList) throws SQLException {
        if (!rs1.next()) {
            return false;
        }
        if (!pkCompare1){
            pkCompare1 = true;
            for (ColumnMapperInfo pki:pkList){
                pki.columnA.read(rs1);
            }
        } else {
            int equalsCount = 0;
            boolean stopCompare = false;
            for (ColumnMapperInfo pki:pkList){
                Object v1 = pki.columnA.value;
                Object v2 = rs1.getObject(pki.columnA.rsIndex);

                int compareResult = compare(v1,v2);
                if (compareResult == 0){
                    equalsCount++;
                } else if (!stopCompare){
                    if (compareResult<0){
                        stopCompare = true;
                    } else {
                        Object[] prevRow = new Object[pkList.length];
                        Object[] currentRow = new Object[pkList.length];
                        for (int j=0; j<pkList.length; ++j) {
                            prevRow[j] = pkList[j].columnA.value;
                            currentRow[j] = rs1.getObject(pkList[j].columnA.rsIndex);
                        }
                        throw new IllegalStateException("Primary key should increment, but the following "+
                                "sequence detected " + Arrays.toString(prevRow) + "/" + Arrays.toString(currentRow)+
                                " in source dataset");
                    }
                }
                pki.columnA.value = v2;
           }
           if (!stopCompare && equalsCount == pkList.length){
               Object[] pkValue = new Object[pkList.length];
               for (int j=0; j<pkList.length; ++j) {
                   pkValue[j] = pkList[j].columnA.value;
               }
               logger.warn("Non uninue primary key for " + Arrays.toString(pkValue) +
                       " in source dataset" );
           }
        }
        return true;
    }

    private boolean nextPk2(ResultSet rs2, ColumnMapperInfo[] pkList) throws SQLException {
        if (!rs2.next()) {
            return false;
        }
        if (!pkCompare2){
            pkCompare2 = true;
            for (ColumnMapperInfo pki:pkList){
                pki.columnB.read(rs2);
            }
        } else {
            int equalsCount = 0;
            boolean stopCompare = false;
            for (ColumnMapperInfo pki:pkList){
                Object v1 = pki.columnB.value;
                Object v2 = rs2.getObject(pki.columnB.rsIndex);

                int compareResult = compare(v1,v2);
                if (compareResult == 0){
                    equalsCount++;
                } else if (!stopCompare){
                    if (compareResult<0){
                        stopCompare = true;
                    } else {
                        Object[] prevRow = new Object[pkList.length];
                        Object[] currentRow = new Object[pkList.length];
                        for (int j=0; j<pkList.length; ++j) {
                            prevRow[j] = pkList[j].columnB.value;
                            currentRow[j] = rs2.getObject(pkList[j].columnB.rsIndex);
                        }
                        throw new IllegalStateException("Primary key should increment, but the following "+
                                "sequence detected " + Arrays.toString(prevRow) + "/" + Arrays.toString(currentRow)+
                                " in target dataset");
                    }
                }
                pki.columnB.value = v2;
           }
           if (!stopCompare && equalsCount == pkList.length){
               Object[] pkValue = new Object[pkList.length];
               for (int j=0; j<pkList.length; ++j) {
                   pkValue[j] = pkList[j].columnB.value;
               }
               logger.warn("Non uninue primary key for " + Arrays.toString(pkValue) +
                       " in target dataset" );
           }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private int comparePk(ColumnMapperInfo[] pkList) {
        for (ColumnMapperInfo cmi:pkList) {
            Comparable<Object> v1 = (Comparable<Object>) cmi.columnA.value;
            Comparable<Object> v2 = (Comparable<Object>) cmi.columnB.value;
            int r = compare(v1,v2);
            if (r != 0) {
                return r;
            }
        }
        return 0;
    }

    @SuppressWarnings("unchecked")
    private int compare(Object pk1, Object pk2) {
        Comparable<Object> v1 = (Comparable<Object>) pk1;
        Comparable<Object> v2 = (Comparable<Object>) pk2;
        return  v1.compareTo(v2);
    }

    private void read2(ResultSet rs2, ColumnMapperInfo[] columnList) throws SQLException {
        for (ColumnMapperInfo cmi:columnList) {
            if (cmi.columnB!=null){
                cmi.columnB.read(rs2);
            }
        }
    }

    private void read1(ResultSet rs1, ColumnMapperInfo[] columnList) throws SQLException {
        for (ColumnMapperInfo cmi:columnList) {
            if (cmi.columnA!=null){
                cmi.columnA.read(rs1);
            }
        }
    }

    protected abstract void onStart(ColumnMapperInfo[] pkList, ColumnMapperInfo[] columnList);
    protected abstract void markAsNew(ColumnMapperInfo[] pkList, ColumnMapperInfo[] columnList);
    protected abstract void markAsDeleted(ColumnMapperInfo[] pkList, ColumnMapperInfo[] columnList);
    protected abstract void markAsChanged(ColumnMapperInfo[] pkList, ColumnMapperInfo[] columnList);
    protected abstract void onStop();
}
