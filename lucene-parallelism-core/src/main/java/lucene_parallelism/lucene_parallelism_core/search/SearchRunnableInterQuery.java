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

public class SearchRunnableInterQuery implements Runnable{
	IndexSearcher searcher;
	TrecTopic topic;
	Query query;
	Filter filter;
	int numResults;
	String runtag;
	
	SearchRunnableInterQuery(IndexSearcher searcher, TrecTopic topic, Query query, Filter filter, int numResults, String runtag) {
		this.searcher = searcher;
		this.topic = topic;
		this.query = query;
		this.filter = filter;
		this.numResults = numResults;
		this.runtag = runtag;
	}
	public void run() {
		try {
			TopDocs rs = searcher.search(query, filter, numResults);
			int i = 1;
			for (ScoreDoc scoreDoc : rs.scoreDocs) {
				Document hit = searcher.doc(scoreDoc.doc);
				// System.out.println(String.format("%s Q0 %s %d %f %s", topic.getId(), hit.getField(StatusField.ID.name).numericValue(), i, scoreDoc.score, runtag));
				i ++;
			}
		} catch(IOException e) {}
	}
}
