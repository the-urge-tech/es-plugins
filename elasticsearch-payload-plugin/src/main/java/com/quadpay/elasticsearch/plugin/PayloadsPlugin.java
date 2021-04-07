package com.quadpay.elasticsearch.plugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScoreScript.LeafFactory;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PayloadsPlugin extends Plugin implements ScriptPlugin {
  private static final Logger logger = LogManager.getLogger(PayloadsScriptEngine.class);

  @Override
  public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
    return new PayloadsScriptEngine();
  }

  private static class PayloadsScriptEngine implements ScriptEngine {
    @Override
    public String getType() {
      return "payload_scripts";
    }

    @Override
    public <T> T compile(String scriptName, String scriptSource, ScriptContext<T> context, Map<String, String> params) {
      if (context.equals(ScoreScript.CONTEXT) == false) {
        throw new IllegalArgumentException(getType() + " scripts cannot be used for context [" + context.name + "]");
      }
      // we use the script "source" as the script identifier
      if ("payloads".equals(scriptSource)) {
        ScoreScript.Factory factory = new PayloadsFactory();
        return context.factoryClazz.cast(factory);
      }
      throw new IllegalArgumentException("Unknown script name " + scriptSource);
    }

    @Override
    public void close() {
    }

    @Override
    public Set<ScriptContext<?>> getSupportedContexts() {
      return Set.of(ScoreScript.CONTEXT);
    }

    private static class PayloadsFactory implements ScoreScript.Factory, ScriptFactory {
      @Override
      public boolean isResultDeterministic() {
        return true;
      }

      @Override
      public LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
        return new PayloadsLeafFactory(params, lookup);
      }
    }

    private static class PayloadsLeafFactory implements ScoreScript.LeafFactory {
      private final Map<String, Object> params;
      private final SearchLookup lookup;
      private final String field;
      private final String term;

      private PayloadsLeafFactory(Map<String, Object> params, SearchLookup lookup) {
        if (params.containsKey("field") == false) {
          throw new IllegalArgumentException("Missing parameter [field]");
        }
        if (params.containsKey("term") == false) {
          throw new IllegalArgumentException("Missing parameter [term]");
        }
        this.params = params;
        this.lookup = lookup;
        field = params.get("field").toString();
        term = params.get("term").toString();
      }

      @Override
      public boolean needs_score() {
        return false;  // Return true if the script needs the score
      }

      @Override
      public ScoreScript newInstance(LeafReaderContext context) throws IOException {
        PostingsEnum postings = context.reader().postings(new Term(field, term), PostingsEnum.PAYLOADS);
        if (postings == null) {
          return new ScoreScript(params, lookup, context) {
            @Override
            public double execute(ExplanationHolder explanation) {
              return 0.0d;
            }
          };
        }

        BytesRef payload = postings.getPayload();

        if (logger.isDebugEnabled()) {
          logger.debug("Has payloads: " + context.reader().terms(field).hasPayloads());
          logger.debug("Postings class: " + postings.getClass().getName());
          logger.debug("Store payloads: " + context.reader().getFieldInfos().fieldInfo(field).hasPayloads());

          logger.debug("Payload: " + context.reader().terms(field).iterator().postings(null, PostingsEnum.PAYLOADS).getPayload());

          logger.debug(Arrays.stream(Thread.currentThread().getStackTrace()).
                  map(stackTraceElement -> stackTraceElement.toString()).collect(Collectors.joining("\n")));
        }

        if (payload == null) {
          logger.info("No payloads for field " + field);
        }

        return new ScoreScript(params, lookup, context) {
          int currentDocid = -1;

          @Override
          public void setDocument(int docid) {
            if (postings.docID() < docid) {
              try {
                postings.advance(docid);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }
            currentDocid = docid;
          }

          @Override
          public double execute(ExplanationHolder explanation) {
            if (postings.docID() != currentDocid) {
              return 0.0d;
            }

            // logger.info("Document " + currentDocid + " " + postings.docID());
            if (payload != null && payload.length < payload.offset) {
              return PayloadHelper.decodeFloat(payload.bytes, payload.offset);
            }
            return 0.0d;
          }
        };
      }
    }
  }
}
