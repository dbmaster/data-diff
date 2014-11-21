package io.dbmaster.tools.diff;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;

class DecoratedCompareData extends CompareData{
    private static final String COLOR_NEW     = "#C4D79B";
    private static final String COLOR_DELETED = "rgb(249, 154, 156)";
    private static final String COLOR_UPDATED = "rgb(255, 252, 185)";
    private static final String COLOR_CHANGED = "#FFFF80";

    private StringBuilder builder;
    private int newCount = 0;
    private int deletedCount = 0;
    private int changedCount = 0;
    private int unchagedCount = 0;
    
    private final boolean trim;
    private final boolean ignoreCase;
    
    private final boolean newFilter;
    private final boolean changedFilter;
    private final boolean deletedFilter;
    private final boolean unchagedFilter;

    public DecoratedCompareData(ResultSet rs1, ResultSet rs2, String compareKey,
            Long limit, Logger logger, PrintWriter printWriter) throws SQLException {
        this(rs1, rs2, compareKey, limit, logger, printWriter, new String[0],new String[0]);
    }
    
    public DecoratedCompareData(ResultSet rs1, ResultSet rs2, String compareKey,
            Long limit, Logger logger, PrintWriter printWriter,
            String[] outputFilters, String[] options) throws SQLException {
        super(rs1, rs2, compareKey, logger, true, printWriter, limit);

        List<String> filter = Arrays.asList(outputFilters);
        this.newFilter = filter.contains("New");
        this.changedFilter = filter.contains("Changed");
        this.deletedFilter = filter.contains("Deleted");
        this.unchagedFilter = filter.contains("Same");
        
        List<String> option = Arrays.asList(options);
        this.trim = option.contains("Trim");
        this.ignoreCase = option.contains("Ignore case");
        
        doCompare();
    }

    protected void onStart(ColumnMapperInfo[] pkList, ColumnMapperInfo[] columnList){
        builder = new StringBuilder(1024);

        printWriter.println("<table cellspacing=\"0\" class=\"simple-table\" border=\"1\">");
        printWriter.println("<tr>");
        printWriter.println("<td style=\"background-color:"+COLOR_NEW+"\">New row/column</td>");
        printWriter.println("<td style=\"background-color:"+COLOR_UPDATED+"\">Updated row</td>");
        printWriter.println("<td style=\"background-color:"+COLOR_DELETED+"\">Deleted row/column</td>");
        printWriter.println("<td>Unchanged row/value</td>");
        printWriter.println("<td style=\"background-color:"+COLOR_CHANGED+"\">Changed value</td>");
        printWriter.println("</tr>");
        printWriter.println("</table>");
        printWriter.println("<br/>");

        printWriter.println("<table cellspacing=\"0\" class=\"simple-table\" border=\"1\">");
        printWriter.println("<tr>");
        for (ColumnMapperInfo pk:pkList){
            printColumnHeader(pk, "PK: ");
        }
        for (ColumnMapperInfo c:columnList){
            printColumnHeader(c, "");
        }
        printWriter.println("</tr>");
    }

    protected void markAsNew(ColumnMapperInfo[] pkList, ColumnMapperInfo[] columnList) {
        newCount++;
        if (newFilter){
            return;
        }
        printWriter.print("<tr style=\"background-color:");
        printWriter.print(COLOR_NEW);
        printWriter.print("\">");
        for (ColumnMapperInfo c:pkList){
            printWriter.println("<td>");
            printWriter.println(c.columnB.value);
            printWriter.println("</td>");
        }
        for (ColumnMapperInfo cmi:columnList){
            if (cmi.columnB==null){
                // old column
                printWriter.println("<td style=\"background-color:");
                printWriter.println(COLOR_DELETED);
                printWriter.println("\">");
                printWriter.println(cmi.columnA.value);
                printWriter.println("</td>");
            } else{
                // normal -> new or new
                printWriter.println("<td>");
                printWriter.println(cmi.columnB.value);
                printWriter.println("</td>");
            }
        }
        printWriter.print("</tr>");
    }

    protected void markAsDeleted(ColumnMapperInfo[] pkList, ColumnMapperInfo[] columnList) {
        deletedCount++;
        if (deletedFilter){
            return;
        }
        printWriter.print("<tr style=\"background-color:");
        printWriter.print(COLOR_DELETED);
        printWriter.print("\">");
        for (ColumnMapperInfo c:pkList){
            printWriter.println("<td>");
            printWriter.println(c.columnA.value);
            printWriter.println("</td>");
        }
        for (ColumnMapperInfo cmi:columnList){
            if (cmi.columnA==null){
                // new column
                printWriter.println("<td style=\"background-color:");
                printWriter.println(COLOR_NEW);
                printWriter.println("\">");
                printWriter.println(cmi.columnB.value);
                printWriter.println("</td>");
            } else {
                // normal -> deleted or deleted
                printWriter.println("<td>");
                printWriter.println(cmi.columnA.value);
                printWriter.println("</td>");
            }
        }
        printWriter.print("</tr>");
    }

    protected void markAsChanged(ColumnMapperInfo[] pkList, ColumnMapperInfo[] columnList) {
        builder.setLength(0);
        for (ColumnMapperInfo ci:pkList){
            builder.append("<td>");
            builder.append(ci.columnA.value);
            builder.append("</td>");
        }

        boolean equals = true;
        for (ColumnMapperInfo ci:columnList){
            if (ci.columnA!=null && ci.columnB!=null){
                // normal
                if (!equalWithNull(ci.columnA.value,ci.columnB.value)){
                    equals = false;
                    builder.append("<td style=\"background-color:");
                    builder.append(COLOR_CHANGED);
                    builder.append("\">");
                    builder.append(ci.columnA.value);
                    builder.append(" -&gt; ");
                    builder.append(ci.columnB.value);
                    builder.append("</td>");
                } else {
                    builder.append("<td>");
                    builder.append(ci.columnA.value);
                    builder.append("</td>");
                }
            } else if (ci.columnA!=null){
                // deleted
                builder.append("<td style=\"background-color:");
                builder.append(COLOR_DELETED);
                builder.append("\">");
                builder.append(ci.columnA.value);
                builder.append("</td>");
            } else {
                // new
                builder.append("<td style=\"background-color:");
                builder.append(COLOR_NEW);
                builder.append("\">");
                builder.append(ci.columnB.value);
                builder.append("</td>");
            }
        }

        if (equals){
            unchagedCount++;
            if (unchagedFilter){
                return;
            }
            printWriter.print("<tr>");
            printWriter.print(builder.toString());
            printWriter.print("</tr>");
           
        } else {
            changedCount++;
            if (changedFilter){
                return;
            }
            printWriter.print("<tr style=\"background-color:");
            printWriter.print(COLOR_UPDATED);
            printWriter.print("\">");
            printWriter.print(builder.toString());
            printWriter.print("</tr>");
        }
    }

    protected void onStop(){
        printWriter.println("</table>");
        
        printWriter.println("<div>Compare statistics:</div>");
        printWriter.println("<table cellspacing=\"0\" class=\"simple-table\" border=\"1\">");
        printWriter.println("<tr>");
        printWriter.println("<td style=\"background-color:"+COLOR_NEW+"\">New row(s) " + newCount + " </td>");
        printWriter.println("<td style=\"background-color:"+COLOR_CHANGED+"\">Changed row(s) " 
                + changedCount + "</td>");
        printWriter.println("<td style=\"background-color:"+COLOR_DELETED+"\">Deleted row(s) " 
                + deletedCount + "</td>");
        printWriter.println("<td>Same row(s) "+ unchagedCount + "</td>");
        printWriter.println("<td>All row(s) "+ ((long)newCount+changedCount+deletedCount+unchagedCount) + "</td>");
        printWriter.println("</tr>");
        printWriter.println("</table>");
    }


    private void printColumnHeader(ColumnMapperInfo info, String prefix){
        if (info.columnA!=null && info.columnB!=null){
            if (info.columnA.type!=info.columnB.type){
                printWriter.println("<td style=\"background-color:");
                printWriter.println(COLOR_CHANGED);
                printWriter.println("\">");
                printName(info, prefix);
                printWriter.println(info.columnA.typeName);
                printWriter.println("/");
                printWriter.println(info.columnA.typeClass);
                printWriter.println(" -&gt; ");
                printWriter.println(info.columnB.typeName);
                printWriter.println("/");
                printWriter.println(info.columnB.typeClass);
                printWriter.println("</td>");
            } else {
                printWriter.println("<td>");
                printName(info, prefix);
                printWriter.println(info.columnA.typeName);
                printWriter.println("/");
                printWriter.println(info.columnB.typeName);
                printWriter.println("</td>");
            }
        } else if (info.columnA!=null){
            // deleted
            printWriter.println("<td style=\"background-color:");
            printWriter.println(COLOR_DELETED);
            printWriter.println("\">");
            printWriter.println(prefix);
            printWriter.println(info.columnA.name);
            printWriter.println("<br/>");
            printWriter.println(info.columnA.typeName);
            printWriter.println("/");
            printWriter.println(info.columnA.typeClass);
            printWriter.println("</td>");
        } else {
            // new
            printWriter.println("<td style=\"background-color:");
            printWriter.println(COLOR_NEW);
            printWriter.println("\">");
            printWriter.println(prefix);
            printWriter.println(info.columnB.name);
            printWriter.println("<br/>");
            printWriter.println(info.columnB.typeName);
            printWriter.println("/");
            printWriter.println(info.columnB.typeClass);
            printWriter.println("</td>");
        }
    }

    private void printName(ColumnMapperInfo info, String prefix) {
        printWriter.println(prefix);
        if (info.columnA.name.equals(info.columnB.name)){
            printWriter.println(info.columnA.name);
        } else {
            printWriter.println(info.columnA.name);
            printWriter.println(" -&gt; ");
            printWriter.println(info.columnB.name);
        }
        printWriter.println("<br/>");
    }

    private boolean equalWithNull(Object obj1, Object obj2) {
        if (obj1 == obj2)
            return true;
        if (obj1 == null) {
            return false;
        }
        if (obj1 instanceof String && obj2 instanceof String){
            if (trim){
                obj1 = ((String) obj1).trim();
                obj2 = ((String) obj2).trim();
            }
            if (ignoreCase){
                return ((String) obj1).equalsIgnoreCase((String) obj2);
            }
        }
        return obj1.equals(obj2);
    }
}

