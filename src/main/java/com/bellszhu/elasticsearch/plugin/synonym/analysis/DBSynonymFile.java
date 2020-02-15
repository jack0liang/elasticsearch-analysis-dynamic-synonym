package com.bellszhu.elasticsearch.plugin.synonym.analysis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.env.Environment;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;

/**
 *
 */
public class DBSynonymFile implements SynonymFile {

    private static Logger logger = LogManager.getLogger("dynamic-synonym");
    private String format;

    private boolean expand;

    private Analyzer analyzer;

    private Environment env;
    private long lastUpdateVersion=0;
    private long tmpUpdateVersion=0;
    /**
     * Remote URL address
     */
    private String location;

    DBSynonymFile(Environment env, Analyzer analyzer,
                      boolean expand, String format, String location) {
        this.analyzer = analyzer;
        this.expand = expand;
        this.format = format;
        this.env = env;
        this.location = location;

        isNeedReloadSynonymMap();
    }

    @Override
    public SynonymMap reloadSynonymMap() {
        try {
            logger.info("start reload db synonym from {}.", location);
            Reader rulesReader = getReader();
            SynonymMap.Builder parser = RemoteSynonymFile.getSynonymParser(rulesReader, format, expand, analyzer);
            lastUpdateVersion = tmpUpdateVersion;
            return parser.build();
        } catch (Exception e) {
            logger.error("reload db synonym {} error!", location, e);
            throw new IllegalArgumentException(
                    "could not reload db synonyms to build synonyms", e);
        }
    }

    @Override
    public boolean isNeedReloadSynonymMap() {
        try {
            long currentMaxVersion = JDBCUtils.queryMaxSynonymRuleVersion(location);
            if (currentMaxVersion > lastUpdateVersion) {
                tmpUpdateVersion = currentMaxVersion;
                return true;
            }
        }catch (Exception e){
            logger.error("",e);
        }
        return false;
    }

    @Override
    public Reader getReader() {
        try {
            List<String> synonymRuleList = JDBCUtils.querySynonymRules(location, tmpUpdateVersion);
            StringBuilder sb = new StringBuilder();
            for (String line : synonymRuleList) {
                logger.info("reload db synonym: {}", line);
                sb.append(line).append(System.getProperty("line.separator"));
            }
            return new StringReader(sb.toString());
        }catch (Exception e){
            logger.error("",e);
        }
        return null;
    }
}
