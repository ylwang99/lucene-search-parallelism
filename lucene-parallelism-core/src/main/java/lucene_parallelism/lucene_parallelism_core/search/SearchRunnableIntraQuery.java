package lucene_parallelism.lucene_parallelism_core.search;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import cc.twittertools.index.IndexStatuses.StatusField;
import cc.twittertools.search.TrecTopic;

public class SearchRunnableIntraQuery implements Runnable{
	IndexSearcher searcher;
	Query query;
	Filter filter;
	int numResults;
	TopNFast topN;
	
	SearchRunnableIntraQuery(IndexSearcher searcher, Query query, Filter filter, int numResults, TopNFast topN) {
		this.searcher = searcher;
		this.query = query;
		this.filter = filter;
		this.numResults = numResults;
		this.topN = topN;
	}
	public void run() {
		try {
			TopDocs rs = searcher.search(query, filter, numResults);
			for (ScoreDoc scoreDoc : rs.scoreDocs) {
				Document hit = searcher.doc(scoreDoc.doc);
				topN.add(hit.getField(StatusField.ID.name).numericValue().longValue(), scoreDoc.score);
			}
		} catch(IOException e) {}
	}
}
