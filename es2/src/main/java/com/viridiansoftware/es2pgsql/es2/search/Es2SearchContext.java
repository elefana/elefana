/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.es2.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.Counter;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.ContextAndHeaderHolder;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.SearchContextAggregations;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSubPhase.ContextFactory;
import org.elasticsearch.search.fetch.FetchSubPhaseContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.scan.ScanContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

public class Es2SearchContext extends SearchContext {
	private long originalNanoTime;
	private long id;
	private String [] types;
	private String source;
	private SearchType searchType;
	private float queryBoost;
	private ScrollContext scrollContext;
	private SearchContextAggregations aggregations;
	private SearchContextHighlight highlight;
	private SuggestionSearchContext suggestion;
	private final List<RescoreSearchContext> rescores = new ArrayList<RescoreSearchContext>();
	private FetchSourceContext fetchSourceContext;
	private long timeoutInMillis;
	private int terminateAfter;
	private float minimumScore;
	private Sort sort;
	private boolean trackScores;
	private ParsedQuery postFilter;
	private ParsedQuery query;
	private int from, size;
	private long accessTime, keepAlive;
	private int[] docIdsToLoad;
	private int docsIdsToLoadFrom, docsIdsToLoadSize;

	public Es2SearchContext() {
		super(ParseFieldMatcher.EMPTY, new ContextAndHeaderHolder());
		this.originalNanoTime = System.nanoTime();
	}

	@Override
	protected void doClose() {
		
	}

	@Override
	public void preProcess() {
		
	}

	@Override
	public Query searchFilter(String[] types) {
		this.types = types;
		return query != null ? query.query() : null;
	}

	@Override
	public long id() {
		return id;
	}

	@Override
	public String source() {
		return source;
	}

	@Override
	public ShardSearchRequest request() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchType searchType() {
		return searchType;
	}

	@Override
	public SearchContext searchType(SearchType searchType) {
		this.searchType = searchType;
		return this;
	}

	@Override
	public SearchShardTarget shardTarget() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int numberOfShards() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean hasTypes() {
		return types != null && types.length > 0;
	}

	@Override
	public String[] types() {
		return types;
	}

	@Override
	public float queryBoost() {
		return queryBoost;
	}

	@Override
	public SearchContext queryBoost(float queryBoost) {
		this.queryBoost = queryBoost;
		return this;
	}

	@Override
	public long getOriginNanoTime() {
		return originalNanoTime;
	}

	@Override
	protected long nowInMillisImpl() {
		return System.currentTimeMillis();
	}

	@Override
	public ScrollContext scrollContext() {
		return scrollContext;
	}

	@Override
	public SearchContext scrollContext(ScrollContext scroll) {
		this.scrollContext = scroll;
		return this;
	}

	@Override
	public SearchContextAggregations aggregations() {
		return aggregations;
	}

	@Override
	public SearchContext aggregations(SearchContextAggregations aggregations) {
		this.aggregations = aggregations;
		return this;
	}

	@Override
	public <SubPhaseContext extends FetchSubPhaseContext> SubPhaseContext getFetchSubPhaseContext(
			ContextFactory<SubPhaseContext> contextFactory) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchContextHighlight highlight() {
		return highlight;
	}

	@Override
	public void highlight(SearchContextHighlight highlight) {
		this.highlight = highlight;
	}

	@Override
	public SuggestionSearchContext suggest() {
		return suggestion;
	}

	@Override
	public void suggest(SuggestionSearchContext suggest) {
		this.suggestion = suggest;
	}

	@Override
	public List<RescoreSearchContext> rescore() {
		return rescores;
	}

	@Override
	public void addRescore(RescoreSearchContext rescore) {
		rescores.add(rescore);
	}

	@Override
	public boolean hasScriptFields() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ScriptFieldsContext scriptFields() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean sourceRequested() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean hasFetchSourceContext() {
		return fetchSourceContext != null;
	}

	@Override
	public FetchSourceContext fetchSourceContext() {
		return fetchSourceContext;
	}

	@Override
	public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
		this.fetchSourceContext = fetchSourceContext;
		return this;
	}

	@Override
	public ContextIndexSearcher searcher() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IndexShard indexShard() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MapperService mapperService() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AnalysisService analysisService() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IndexQueryParserService queryParserService() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SimilarityService similarityService() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ScriptService scriptService() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PageCacheRecycler pageCacheRecycler() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigArrays bigArrays() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BitsetFilterCache bitsetFilterCache() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IndexFieldDataService fieldData() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long timeoutInMillis() {
		return timeoutInMillis;
	}

	@Override
	public void timeoutInMillis(long timeoutInMillis) {
		this.timeoutInMillis = timeoutInMillis;
	}

	@Override
	public int terminateAfter() {
		return terminateAfter;
	}

	@Override
	public void terminateAfter(int terminateAfter) {
		this.terminateAfter = terminateAfter;
	}

	@Override
	public SearchContext minimumScore(float minimumScore) {
		this.minimumScore = minimumScore;
		return this;
	}

	@Override
	public Float minimumScore() {
		return minimumScore;
	}

	@Override
	public SearchContext sort(Sort sort) {
		this.sort = sort;
		return this;
	}

	@Override
	public Sort sort() {
		return sort;
	}

	@Override
	public SearchContext trackScores(boolean trackScores) {
		this.trackScores = trackScores;
		return this;
	}

	@Override
	public boolean trackScores() {
		return trackScores;
	}

	@Override
	public SearchContext parsedPostFilter(ParsedQuery postFilter) {
		this.postFilter = postFilter;
		return this;
	}

	@Override
	public ParsedQuery parsedPostFilter() {
		return postFilter;
	}

	@Override
	public Query aliasFilter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SearchContext parsedQuery(ParsedQuery query) {
		this.query = query;
		return this;
	}

	@Override
	public ParsedQuery parsedQuery() {
		return query;
	}

	@Override
	public Query query() {
		if(query == null) {
			return null;
		}
		return query.query();
	}

	@Override
	public int from() {
		return from;
	}

	@Override
	public SearchContext from(int from) {
		this.from = from;
		return this;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public SearchContext size(int size) {
		this.size = size;
		return this;
	}

	@Override
	public boolean hasFieldNames() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<String> fieldNames() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void emptyFieldNames() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean explain() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void explain(boolean explain) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<String> groupStats() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void groupStats(List<String> groupStats) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean version() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void version(boolean version) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int[] docIdsToLoad() {
		return docIdsToLoad;
	}

	@Override
	public int docIdsToLoadFrom() {
		return docsIdsToLoadFrom;
	}

	@Override
	public int docIdsToLoadSize() {
		return docsIdsToLoadSize;
	}

	@Override
	public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
		this.docIdsToLoad = docIdsToLoad;
		this.docsIdsToLoadFrom = docsIdsToLoadFrom;
		this.docsIdsToLoadSize = docsIdsToLoadSize;
		return this;
	}

	@Override
	public void accessed(long accessTime) {
		this.accessTime = accessTime;
	}

	@Override
	public long lastAccessTime() {
		return accessTime;
	}

	@Override
	public long keepAlive() {
		return keepAlive;
	}

	@Override
	public void keepAlive(long keepAlive) {
		this.keepAlive = keepAlive;
	}

	@Override
	public SearchLookup lookup() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DfsSearchResult dfsResult() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QuerySearchResult queryResult() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FetchSearchResult fetchResult() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Profilers getProfilers() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ScanContext scanContext() {
		return null;
	}

	@Override
	public MappedFieldType smartNameFieldType(String name) {
		return null;
	}

	@Override
	public MappedFieldType smartNameFieldTypeFromAnyType(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectMapper getObjectMapper(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Counter timeEstimateCounter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<Class<?>, Collector> queryCollectors() {
		// TODO Auto-generated method stub
		return null;
	}

}
