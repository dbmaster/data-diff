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
import com.branegy.files.FileService
import com.branegy.files.FileReference

def source_server    = p_source_db.split("\\.")[0]
def source_database  = p_source_db.split("\\.")[1]
def target_server    = p_target_db.split("\\.")[0]
def target_database  = p_target_db.split("\\.")[1]

connectionSrv = dbm.getService(ConnectionService.class)

connectionInfo1 = connectionSrv.findByName(source_server)
connection1 = ConnectionProvider.getConnector(connectionInfo1).getJdbcConnection(source_database)
logger.info("Connecting to ${source_server}"+(source_database==null?"":source_database))
connectionInfo2 = connectionSrv.findByName(target_server)
connection2 = ConnectionProvider.getConnector(connectionInfo2).getJdbcConnection(target_database)
logger.info("Connecting to ${target_server}"+(target_database==null?"":target_database))
dbm.closeResourceOnExit(connection1)
dbm.closeResourceOnExit(connection2)

def printInfo(PrintWriter pw, source_server, source_database, target_server, target_database) {
    pw.println( "<table cellspacing=\"0\" class=\"simple-table\" border=\"1\">");
    pw.println( "<tr><td>Source Server</td><td>${source_server}</td></tr>");
    pw.println( "<tr><td>Source Database</td><td>"+(source_database==null?"":source_database)+"</td></tr>");
    pw.println( "<tr><td>Target Server</td><td>${target_server}</td></tr>");
    pw.println( "<tr><td>Target Database</td><td>"+(target_database==null?"":target_database)+"</td></tr>");
    pw.println( "<tr><td>Compare Key</td><td>${p_pk}</td></tr>");
    pw.println( "<tr><td>Source Query</td><td>${p_source_sql}</td></tr>");
    pw.println( "<tr><td>Target Query</td><td>${p_target_sql}</td></tr>");
    pw.println( "</table><br/>");
}

if ("File".equals(p_target)) {
    FileService fileService = dbm.getService(FileService.class)

    def fileName = "data_diff_${(new Date()).format("yyyyMMdd_HHmmss")}.html"
    def file = fileService.createFile(fileName,"db-data-diff")

    print "<a target=\"_blank\" href=\"${fileService.toURL(file)}\">Download diff results</a>"
    def outStream = file.getOutputStream()
    try {
        PrintWriter pw = new PrintWriter(outStream)
        dbm.closeResourceOnExit(pw)
        printInfo(pw, source_server, source_database, target_server, target_database)
        io.dbmaster.tools.diff.CompareData.executeAsync(connection1, connection2, p_source_sql, p_target_sql, p_pk, p_limit, logger, pw, p_output_filters, p_options)
        pw.flush();
    } finally { 
        if (outStream!=null) { 
            outStream.close();
        }
    }
} else {
    StringWriter sw = new StringWriter(2*1024*1024)
    PrintWriter pw = new PrintWriter(sw)
    printInfo(pw, source_server, source_database, target_server, target_database)
    io.dbmaster.tools.diff.CompareData.executeAsync(connection1, connection2, p_source_sql, p_target_sql, p_pk, p_limit, logger, pw, p_output_filters, p_options)
    pw.flush();
    print sw.toString()
}