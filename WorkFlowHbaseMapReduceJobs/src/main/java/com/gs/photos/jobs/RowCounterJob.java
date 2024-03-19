package com.gs.photos.jobs;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import com.gsphotos.worflow.hbasefilters.FilterRowByLongAtAGivenOffset;

public class RowCounterJob extends Job implements IRowCounterJob, Callable<Integer> {

	private static final String NAME              = "rowcounter";
	private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";
	private LocalDateTime       startCreationDate;
	private LocalDateTime       endCreationDate;

	protected String            tableName;

	static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {

		/** Counter enumeration to count the actual rows. */
		public static enum Counters {
			ROWS
		}

		/**
		 * Maps the data.
		 *
		 * @param row
		 *            The current table row key.
		 * @param values
		 *            The columns.
		 * @param context
		 *            The current context.
		 * @throws IOException
		 *             When something is broken with the data.
		 * @see org.apache.hadoop.mapreduce.Mapper#map(Object, Object, Context)
		 */
		@Override
		public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
			// Count every row containing data, whether it's in qualifiers or values
			context.getCounter(Counters.ROWS).increment(1);
		}
	}

	protected Token<?>[] obtainSystemTokensForUser(String user, final Credentials credentials)
		throws IOException, InterruptedException {
		// Get new hdfs tokens on behalf of this user
		UserGroupInformation proxyUser = UserGroupInformation.getLoginUser();

		Token<?>[] newTokens = proxyUser.doAs((PrivilegedExceptionAction<Token<?>[]>) () -> {
			FileSystem fs = FileSystem.get(HBaseConfiguration.create());
			try {
				return fs.addDelegationTokens("yarn",
					credentials);
			} finally {
				// Close the FileSystem created by the new proxy user,
				// So that we don't leave an entry in the FileSystem cache
				fs.close();
			}
		});
		return newTokens;
	}

	@Override
	public Integer call() {
		try {
			Configuration config = HBaseConfiguration.create();
			Job job = Job.getInstance(config,
				"Row-counter-job");
			JobConf conf = new JobConf(config, this.getClass());
			job.setJarByClass(RowCounterJob.class);
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			final Filter filter = this.getFilters();
			if (filter != null) {
				scan.setFilter(new FilterList(filter, new FirstKeyOnlyFilter()));
			} else {
				scan.setFilter(new FirstKeyOnlyFilter());
			}
			Token<?>[] newTokens = this.obtainSystemTokensForUser("wf_hbase@GS.COM",
				this.credentials);
			for (Token<?> token : newTokens) {
				token.setService(new Text("10.0.0.5:9000"));
				job.getCredentials()
					.addToken(token.getService(),
						token);
			}
			TableMapReduceUtil.initTableMapperJob(this.tableName,
				scan,
				RowCounterMapper.class,
				ImmutableBytesWritable.class,
				Result.class,
				job);
			// FileOutputFormat.setOutputPath(job,
			// new Path("/wf_hbase"));
			// job.getConfiguration()
			// .set("mapreduce.output.fileoutputformat.outputdir",
			// "hdfs://hadoop-master:9000/wf_hbase/jobs");
			boolean success = job.waitForCompletion(true);
			if (success) {
				final Counter counter = job.getCounters().findCounter(RowCounterMapper.Counters.ROWS);
				return (int) counter.getValue();
			} else {
				return (int) -1;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return -1;
	}

	protected Filter getFilters() {
		final long firstDateEpochMillis = this.startCreationDate.toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli();
		final long lastDateEpochMilli = this.endCreationDate.toInstant(ZoneOffset.ofTotalSeconds(0)).toEpochMilli();
		final FilterRowByLongAtAGivenOffset filterRowByLongAtAGivenOffset = new FilterRowByLongAtAGivenOffset(0,
			firstDateEpochMillis,
			lastDateEpochMilli);
		return filterRowByLongAtAGivenOffset;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	public RowCounterJob(
		JobConf conf,
		LocalDateTime startCreationDate,
		LocalDateTime endCreationDate,
		String tableName
	) throws IOException {
		super(conf);
		this.startCreationDate = startCreationDate;
		this.endCreationDate = endCreationDate;
		this.tableName = tableName;
	}

}
