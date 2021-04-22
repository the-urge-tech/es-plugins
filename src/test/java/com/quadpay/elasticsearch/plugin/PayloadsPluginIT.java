package com.quadpay.elasticsearch.plugin;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.payloads.FloatEncoder;
import org.apache.lucene.analysis.payloads.IdentityEncoder;
import org.apache.lucene.analysis.payloads.IntegerEncoder;
import org.apache.lucene.analysis.payloads.PayloadEncoder;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.analysis.PreConfiguredTokenizer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, transportClientRatio = 0, numDataNodes = 1, numClientNodes = 0)
public class PayloadsPluginIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
                MockPayloadAnalyzerPlugin.class,
                PayloadsPlugin.class
        );
    }

    public void testSimplePayload() throws IOException {
        Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.analysis.analyzer.payload_analyzer.tokenizer", "mock-whitespace")
                .putList("index.analysis.analyzer.payload_analyzer.filter", "delimited_payload")
                .put("index.analysis.filter.delimited_payload.encoding", "float")
                .put("index.analysis.filter.delimited_payload.type", "mock_payload_filter").build();


        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("name")
                    .field("type", "text")
                    .field("analyzer", "payload_analyzer")
                .endObject()
            .endObject()
        .endObject();

        assertAcked(prepareCreate("test1")
                .setSettings(settings)
                .addMapping("_doc", mapping)
        );

        ensureGreen("test1");

        client().prepareIndex("test1", "_doc", "1")
                .setSource(jsonBuilder().startObject().field("name", "foo|123 bar|2").endObject())
                .get();

        client().prepareIndex("test1", "_doc", "2")
                .setSource(jsonBuilder().startObject().field("name", "foo|10000000 bar|123 baz").endObject())
                .get();

        client().prepareIndex("test1", "_doc", "3")
                .setSource(jsonBuilder().startObject().field("name", "foo|0").endObject())
                .get();

        refresh();


        Map<String, Object> params = new HashMap<>();
        params.put("field", "name");
        params.put("term", "foo");
        SearchResponse search = client().prepareSearch().
                setQuery(QueryBuilders.functionScoreQuery(
                        ScoreFunctionBuilders.scriptFunction(
                                new Script(
                                        ScriptType.INLINE,
                                        "payload",
                                        "payload",
                                        params
                                )
                        )
                )).get();
        assertHitCount(search, 3);
        assertEquals("2", search.getHits().getHits()[0].getId());
        assertEquals("1", search.getHits().getHits()[1].getId());
        assertEquals("3", search.getHits().getHits()[2].getId());
        assertTrue(search.getHits().getHits()[0].getScore() > 1000);
        assertTrue(search.getHits().getHits()[1].getScore() > 10);
        assertEquals(0, search.getHits().getHits()[2].getScore(), 0.1);
    }

    public static class MockPayloadAnalyzerPlugin extends Plugin implements AnalysisPlugin {

        @Override
        public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
            return Collections.singletonMap("mock_payload_filter", (indexSettings, environment, name, settings) -> {
                return new TokenFilterFactory() {
                    @Override
                    public String name() {
                        return "mock_payload_filter";
                    }

                    @Override
                    public TokenStream create(TokenStream tokenStream) {
                        String delimiter = "|";
                        PayloadEncoder encoder = null;
                        if (settings.get("encoding").equals("float")) {
                            encoder = new FloatEncoder();
                        } else if (settings.get("encoding").equals("int")) {
                            encoder = new IntegerEncoder();
                        } else if (settings.get("encoding").equals("identity")) {
                            encoder = new IdentityEncoder();
                        }
                        return new MockPayloadTokenFilter(tokenStream, delimiter.charAt(0), encoder);
                    }
                };
            });
        }

        @Override
        public List<PreConfiguredTokenizer> getPreConfiguredTokenizers() {
            return Collections.singletonList(PreConfiguredTokenizer.singleton("mock-whitespace",
                    () -> new MockTokenizer(MockTokenizer.WHITESPACE, false)));
        }

        // Based on DelimitedPayloadTokenFilter:
        final class MockPayloadTokenFilter extends TokenFilter {
            private final char delimiter;
            private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
            private final PayloadAttribute payAtt = addAttribute(PayloadAttribute.class);
            private final PayloadEncoder encoder;


            MockPayloadTokenFilter(TokenStream input, char delimiter, PayloadEncoder encoder) {
                super(input);
                this.delimiter = delimiter;
                this.encoder = encoder;
            }

            @Override
            public boolean incrementToken() throws IOException {
                if (input.incrementToken()) {
                    final char[] buffer = termAtt.buffer();
                    final int length = termAtt.length();
                    for (int i = 0; i < length; i++) {
                        if (buffer[i] == delimiter) {
                            payAtt.setPayload(encoder.encode(buffer, i + 1, (length - (i + 1))));
                            termAtt.setLength(i); // simply set a new length
                            return true;
                        }
                    }
                    // we have not seen the delimiter
                    payAtt.setPayload(null);
                    return true;
                } else {
                    return false;
                }
            }
        }
    }

}
