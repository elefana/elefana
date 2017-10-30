/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es2.search;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.AggregationParseElement;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.aggregations.bucket.children.ChildrenParser;
import org.elasticsearch.search.aggregations.bucket.filter.FilterParser;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersParser;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridParser;
import org.elasticsearch.search.aggregations.bucket.global.GlobalParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramParser;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramParser;
import org.elasticsearch.search.aggregations.bucket.missing.MissingParser;
import org.elasticsearch.search.aggregations.bucket.nested.NestedParser;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedParser;
import org.elasticsearch.search.aggregations.bucket.range.RangeParser;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeParser;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistanceParser;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.IpRangeParser;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerParser;
import org.elasticsearch.search.aggregations.bucket.terms.TermsParser;
import org.elasticsearch.search.aggregations.metrics.avg.AvgParser;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityParser;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsParser;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroidParser;
import org.elasticsearch.search.aggregations.metrics.max.MaxParser;
import org.elasticsearch.search.aggregations.metrics.min.MinParser;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksParser;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesParser;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricParser;
import org.elasticsearch.search.aggregations.metrics.stats.StatsParser;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsParser;
import org.elasticsearch.search.aggregations.metrics.sum.SumParser;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.profile.ProfileParseElement;
import org.elasticsearch.search.query.FilterBinaryParseElement;
import org.elasticsearch.search.query.FromParseElement;
import org.elasticsearch.search.query.IndicesBoostParseElement;
import org.elasticsearch.search.query.MinScoreParseElement;
import org.elasticsearch.search.query.PostFilterParseElement;
import org.elasticsearch.search.query.QueryBinaryParseElement;
import org.elasticsearch.search.query.QueryParseElement;
import org.elasticsearch.search.query.SizeParseElement;
import org.elasticsearch.search.query.TerminateAfterParseElement;
import org.elasticsearch.search.query.TimeoutParseElement;
import org.elasticsearch.search.sort.SortParseElement;
import org.elasticsearch.search.sort.TrackScoresParseElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;

import com.viridiansoftware.es2pgsql.es2.search.query.Es2QuerySpec;
import com.viridiansoftware.es2pgsql.node.NodeInfoService;
import com.viridiansoftware.es2pgsql.search.RequestBodySearch;
import com.viridiansoftware.es2pgsql.search.agg.AggregationsParser;

public class Es2RequestBodySearch extends RequestBodySearch {
	private static final Logger LOGGER = LoggerFactory.getLogger(Es2RequestBodySearch.class);
	private static final Map<String, ? extends SearchParseElement> PARSERS = createParseElements();
	
	private final Es2SearchContext searchContext = new Es2SearchContext();

	public Es2RequestBodySearch(HttpEntity<String> httpRequest) throws Exception {
		this(httpRequest, false);
	}
	
	public Es2RequestBodySearch(HttpEntity<String> httpRequest, boolean debug) throws Exception {
		super(httpRequest.getBody(), debug);
		LOGGER.info(httpRequest.getBody());
		parseRequest(searchContext, httpRequest.getBody());
		
		try {
			
			if(debug) {
				System.out.println(searchContext.from());
				System.out.println(searchContext.size());
				System.out.println(searchContext.parsedQuery().toString());
				System.out.println(searchContext.aggregations().toString());
			}
			
			from = searchContext.from() < 0 ? 0 : searchContext.from();
			size = searchContext.size() < 0 ? 10 : searchContext.size();
			
			if(searchContext.parsedQuery() != null) {
				queryTranslator = Es2QuerySpec.parseQuery(searchContext.parsedQuery().query());
			} else {
				queryTranslator = null;
			}
			aggregations.setSubAggregations(AggregationsParser.parseAggregations(httpRequest.getBody()));
			querySqlWhereClause = queryTranslator.toSqlWhereClause();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	private void parseRequest(Es2SearchContext searchContext, String requestBody) throws Exception {
		
		XContentParser parser = XContentFactory.xContent(requestBody).createParser(requestBody);
        XContentParser.Token token;
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse search source. source must be an object, but found [{}] instead", token.name());
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                parser.nextToken();
                SearchParseElement element = PARSERS.get(fieldName);
                if (element == null) {
                    throw new SearchParseException(searchContext, "failed to parse search source. unknown search element [" + fieldName + "]", parser.getTokenLocation());
                }
                element.parse(parser, searchContext);
            } else {
                if (token == null) {
                    throw new ElasticsearchParseException("failed to parse search source. end of query source reached but query is not complete.");
                } else {
                    throw new ElasticsearchParseException("failed to parse search source. expected field name but got [{}]", token);
                }
            }
        }
	}
	
	private static Map<String, ? extends SearchParseElement> createParseElements() {
		final Map<String, SearchParseElement> result = new HashMap<String, SearchParseElement>();
		result.put("from", new FromParseElement());
		result.put("size", new SizeParseElement());
		result.put("indices_boost", new IndicesBoostParseElement());
		result.put("indicesBoost", new IndicesBoostParseElement());
		result.put("query", new QueryParseElement());
		result.put("queryBinary", new QueryBinaryParseElement());
		result.put("query_binary", new QueryBinaryParseElement());
		result.put("filter", new PostFilterParseElement());
		result.put("post_filter", new PostFilterParseElement());
		result.put("postFilter", new PostFilterParseElement());
		result.put("filterBinary", new FilterBinaryParseElement());
		result.put("filter_binary", new FilterBinaryParseElement());
		result.put("sort", new SortParseElement());
		result.put("trackScores", new TrackScoresParseElement());
		result.put("track_scores", new TrackScoresParseElement());
		result.put("min_score", new MinScoreParseElement());
		result.put("minScore", new MinScoreParseElement());
		result.put("timeout", new TimeoutParseElement());
		result.put("terminate_after", new TerminateAfterParseElement());
		result.put("profile", new ProfileParseElement());
		return result;
	}
}