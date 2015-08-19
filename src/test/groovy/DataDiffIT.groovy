import io.dbmaster.testng.BaseToolTestNGCase;

import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test

import com.branegy.tools.api.ExportType;


public class DataDiffIT extends BaseToolTestNGCase {

    @Test
    public void test() {
        def parameters = [ "p_source_db"  :  getTestProperty("data-diff.p_source_db"),
                           "p_target_db"  :  getTestProperty("data-diff.p_target_db"),
                           "p_pk"         :  getTestProperty("data-diff.p_pk"),
                           "p_source_sql" :  getTestProperty("data-diff.p_source_sql"),
                           "p_target_sql" :  getTestProperty("data-diff.p_target_sql")
                         ]
        String result = tools.toolExecutor("data-diff", parameters).execute()
        assertTrue(result.contains("Contact"), "Unexpected search results ${result}");
        assertTrue(result.contains("Related to"), "Unexpected search results ${result}");
    }
}
