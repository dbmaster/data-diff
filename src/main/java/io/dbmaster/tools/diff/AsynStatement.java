package io.dbmaster.tools.diff;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.slf4j.Logger;

import com.branegy.scripting.DbMaster;

// TODO (Vitali) replace CyclicBarrier ?
class AsynStatement extends Thread {
    final Connection connection;
    final String sql;
    final CyclicBarrier barrier;
    final Logger logger;
    final String name;
    final DbMaster dbm;

    volatile ResultSet rs;
    volatile Exception e;

    public AsynStatement(CyclicBarrier barrier, Connection connection, DbMaster dbm, String sql,
            Logger logger, String name) 
    {
        this.barrier = barrier;
        this.connection = connection;
        this.dbm = dbm;
        this.sql = sql;
        this.logger = logger;
        this.name = name;
    }

    @Override
    public void run() {
        try {
            Statement statement = connection.createStatement();
            dbm.closeResourceOnExit(statement);
            rs = statement.executeQuery(sql);
            logger.info("Query " + name + " has completed");
        } catch (SQLException e) {
            this.e = e;
            logger.error("Query " + name + " has completed", e);
            barrier.reset();
            return;
        }
        try {
            barrier.await();
        } catch (InterruptedException ex) {
            return;
        } catch (BrokenBarrierException ex) {
            return;
        }
    }

    public ResultSet getRs() {
        return rs;
    }

    public Exception isException() {
        return e;
    }

}
