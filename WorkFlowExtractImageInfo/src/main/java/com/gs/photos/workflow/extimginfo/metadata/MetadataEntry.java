package com.gs.photos.workflow.extimginfo.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class MetadataEntry {

	private String key;
	private String value;
	private boolean isMetadataEntryGroup;

	private Collection<MetadataEntry> entries = new ArrayList<MetadataEntry>();

	public MetadataEntry(String key, String value) {
		this(key, value, false);
	}

	public MetadataEntry(String key, String value, boolean isMetadataEntryGroup) {
		this.key = key;
		this.value = value;
		this.isMetadataEntryGroup = isMetadataEntryGroup;
	}

	public void addEntry(MetadataEntry entry) {
		entries.add(entry);
	}

	public void addEntries(Collection<MetadataEntry> newEntries) {
		entries.addAll(newEntries);
	}

	public String getKey() {
		return key;
	}

	public boolean isMetadataEntryGroup() {
		return isMetadataEntryGroup;
	}

	public Collection<MetadataEntry> getMetadataEntries() {
		return Collections.unmodifiableCollection(entries);
	}

	public String getValue() {
		return value;
	}
}