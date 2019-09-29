package com.gsphotos.worflow.hbasefilters;

import java.util.ArrayList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.protobuf.generated.GsFilterProtos;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.InvalidProtocolBufferException;

public class FilterRowByLongAtAGivenOffset extends FilterBase {
	private boolean filterOutRow = false;
	private long longOffset;
	private long firstLong;
	private long lastLong;

	public FilterRowByLongAtAGivenOffset(
			long longOffset,
			long firstLong,
			long lastLong) {
		super();
		this.longOffset = longOffset;
		this.firstLong = firstLong;
		this.lastLong = lastLong;
	}

	public FilterRowByLongAtAGivenOffset() {
		super();
	}

	@Override
	public void reset() {
		this.filterOutRow = false;
	}

	@Override
	public ReturnCode filterKeyValue(Cell v) {
		if (this.filterOutRow) {
			return ReturnCode.NEXT_ROW;
		}
		return ReturnCode.INCLUDE;
	}

	@Override
	public boolean filterRowKey(byte[] data, int offset, int length) {
		long value = Bytes.toLongUnsafe(
			data,
			offset + (int) longOffset);
		this.filterOutRow = !(value >= firstLong && value <= lastLong);
		return this.filterOutRow;
	}

	GsFilterProtos.FilterRowByLongAtAGivenOffset convert() {
		GsFilterProtos.FilterRowByLongAtAGivenOffset.Builder builder = GsFilterProtos.FilterRowByLongAtAGivenOffset
				.newBuilder();
		builder.setFirstLong(
			firstLong);
		builder.setLastLong(
			lastLong);
		builder.setLongOffset(
			longOffset);
		return builder.build();
	}

	public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
		@SuppressWarnings("rawtypes") // for arguments
		long longOffset = ParseFilter.convertByteArrayToLong(
			filterArguments.get(
				0));
		long firstLong = ParseFilter.convertByteArrayToLong(
			filterArguments.get(
				1));
		long lastLong = ParseFilter.convertByteArrayToLong(
			filterArguments.get(
				2));

		return new FilterRowByLongAtAGivenOffset(longOffset, firstLong, lastLong);
	}

	/**
	 * @return The filter serialized using pb
	 */
	@Override
	public byte[] toByteArray() {
		GsFilterProtos.FilterRowByLongAtAGivenOffset.Builder builder = GsFilterProtos.FilterRowByLongAtAGivenOffset
				.newBuilder();
		builder.setFirstLong(
			firstLong);
		builder.setLastLong(
			lastLong);
		builder.setLongOffset(
			longOffset);
		return builder.build().toByteArray();
	}

	/**
	 * @param pbBytes
	 *            A pb serialized {@link RowFilter} instance
	 * @return An instance of {@link RowFilter} made from <code>bytes</code>
	 * @throws DeserializationException
	 * @see #toByteArray
	 */
	public static FilterRowByLongAtAGivenOffset parseFrom(final byte[] pbBytes) throws DeserializationException {
		GsFilterProtos.FilterRowByLongAtAGivenOffset proto;
		try {
			proto = GsFilterProtos.FilterRowByLongAtAGivenOffset.parseFrom(
				pbBytes);
		} catch (InvalidProtocolBufferException e) {
			throw new DeserializationException(e);
		}

		return new FilterRowByLongAtAGivenOffset(proto.getLongOffset(), proto.getFirstLong(), proto.getLastLong());
	}

	@Override
	public String toString() {
		return "FilterRowByLongAtAGivenOffset [filterOutRow=" + filterOutRow + ", longOffset=" + longOffset
				+ ", firstLong=" + firstLong + ", lastLong=" + lastLong + "]";
	}

}
