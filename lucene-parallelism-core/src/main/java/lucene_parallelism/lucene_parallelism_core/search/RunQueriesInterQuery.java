/* Run Queries inter query parallelism
 * Usage: sh target/appassembler/bin/RunQueriesInterQuery -index [] -queries [] -threads []
 */
package lucene_parallelism.lucene_parallelism_core.search;

import java.io.File;
import java.io.PrintStream;
import java.io.IOException;
import java.util.List;
import java.io.StringReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import com.google.common.collect.Lists;

import cc.twittertools.index.IndexStatuses;
import cc.twittertools.index.IndexStatuses.StatusField;
import cc.twittertools.search.TrecTopic;
import cc.twittertools.search.TrecTopicSet;

public class RunQueriesInterQuery {
	private static final String DEFAULT_RUNTAG = "lucene4lm_multithread_interquery";

	private static final String INDEX_OPTION = "index";
	private static final String QUERIES_OPTION = "queries";
	private static final String NUM_RESULTS_OPTION = "num_results";
	private static final String SIMILARITY_OPTION = "similarity";
	private static final String RUNTAG_OPTION = "runtag";
	private static final String NTHREADS_OPTION = "threads";

	private RunQueriesInterQuery() {}

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("index location").create(INDEX_OPTION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("file containing topics in TREC format").create(QUERIES_OPTION));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("number of results to return").create(NUM_RESULTS_OPTION));
		options.addOption(OptionBuilder.withArgName("similarity").hasArg()
				.withDescription("similarity to use (BM25, LM)").create(SIMILARITY_OPTION));
		options.addOption(OptionBuilder.withArgName("string").hasArg()
				.withDescription("runtag").create(RUNTAG_OPTION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("number of threads").create(NTHREADS_OPTION));

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(QUERIES_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(RunQueriesInterQuery.class.getName(), options);
			System.exit(-1);
		}

		File indexLocation = new File(cmdline.getOptionValue(INDEX_OPTION));
		if (!indexLocation.exists()) {
			System.err.println("Error: " + indexLocation + " does not exist!");
			System.exit(-1);
		}

		String runtag = cmdline.hasOption(RUNTAG_OPTION) ? cmdline.getOptionValue(RUNTAG_OPTION) : DEFAULT_RUNTAG;
		
		String topicsFile = cmdline.getOptionValue(QUERIES_OPTION);

		int numResults = 1000;
		try {
			if (cmdline.hasOption(NUM_RESULTS_OPTION)) {
				numResults = Integer.parseInt(cmdline.getOptionValue(NUM_RESULTS_OPTION));
			}
		} catch (NumberFormatException e) {
			System.err.println("Invalid " + NUM_RESULTS_OPTION + ": " + cmdline.getOptionValue(NUM_RESULTS_OPTION));
			System.exit(-1);
		}

		String similarity = "LM";
		if (cmdline.hasOption(SIMILARITY_OPTION)) {
			similarity = cmdline.getOptionValue(SIMILARITY_OPTION);
		}

		PrintStream out = new PrintStream(System.out, true, "UTF-8");

		int nThreads = cmdline.hasOption(NTHREADS_OPTION) ? Integer.parseInt(cmdline.getOptionValue(NTHREADS_OPTION)) : 1;
		out.println("Number of threads: " + nThreads);
		IndexReader reader = DirectoryReader.open(FSDirectory.open(indexLocation));
		IndexSearcher searcher = new IndexSearcher(reader);
		
		if (similarity.equalsIgnoreCase("BM25")) {
			searcher.setSimilarity(new BM25Similarity());
		} else if (similarity.equalsIgnoreCase("LM")) {
			searcher.setSimilarity(new LMDirichletSimilarity(2500.0f));
		}

		QueryParser p = new QueryParser(Version.LUCENE_43, StatusField.TEXT.name, IndexStatuses.ANALYZER);
		TrecTopicSet topics = TrecTopicSet.fromFile(new File(topicsFile));

		int N = 4;
		double totalTime = 0;
		int queryCount = 0;
		int topicIdx = 0;
		for (TrecTopic topic : topics) {
			topicIdx ++;
		}
		Query[] query = new Query[topicIdx];
		Filter[] filter = new Filter[topicIdx];
		topicIdx = 0;
		for (TrecTopic topic : topics) {
			query[topicIdx] = p.parse(topic.getQuery());
			filter[topicIdx] = NumericRangeFilter.newLongRange(StatusField.ID.name, 0L, topic.getQueryTweetTime(), true, true);
			topicIdx ++;
		}
		for (int count = 1; count <= N; count ++) {
			topicIdx = 0;
			ExecutorService executor = Executors.newFixedThreadPool(nThreads);
			
			double startTime = System.currentTimeMillis();
			for (TrecTopic topic : topics) {
				queryCount ++;
				// Query query = p.parse(topic.getQuery());
				// Filter filter = NumericRangeFilter.newLongRange(StatusField.ID.name, 0L, topic.getQueryTweetTime(), true, true);
				
				// Runnable worker = new SearchRunnableInterQuery(searcher, topic, query, filter, numResults, runtag);
				Runnable worker = new SearchRunnableInterQuery(searcher, topic, query[topicIdx], filter[topicIdx ++], numResults, runtag);
				executor.execute(worker);
			}
			executor.shutdown();
			while (!executor.isTerminated());

			double endTime = System.currentTimeMillis();
			if (count != 1) {
				totalTime += endTime - startTime;
				out.println("Time = " + (endTime - startTime) / (queryCount / count) + " ms");
			}
		}
		// out.println("total time = " + (totalTime / (N - 1)) + " ms");
	 	out.println("Throughput = " + queryCount / N * 1000.0 / (totalTime / (N - 1)) + " qps");
		reader.close();
		out.close();
	}

	public static List<String> parse(Analyzer analyzer, String s) throws IOException {
		List<String> list = Lists.newArrayList();

		TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(s));
		CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
		tokenStream.reset();
		while (tokenStream.incrementToken()) {
			list.add(cattr.toString());
		}
		tokenStream.end();
		tokenStream.close();

		return list;
	}
}
