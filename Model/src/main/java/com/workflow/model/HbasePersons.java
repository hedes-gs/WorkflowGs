package com.workflow.model;

import java.util.Objects;

@HbaseTableName(value = "persons")
public class HbasePersons extends HbaseData {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Column(hbaseName = "person", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_PERSON_NAME)
    protected String          person;

    @Column(hbaseName = "nbOfElements", rowKeyNumber = 101, toByte = ToByteLong.class, columnFamily = "infos")
    protected long            nbOfElements;

    private HbasePersons(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.person = builder.person;
        this.nbOfElements = builder.nbOfElements;
    }

    public HbasePersons() { super(); }

    public HbasePersons(
        String dataId,
        long dataCreationDate
    ) { super(dataId,
        dataCreationDate); }

    public String getPerson() { return this.person; }

    public void setPerson(String person) { this.person = person; }

    public long getNbOfElements() { return this.nbOfElements; }

    public void setNbOfElements(long nbOfElements) { this.nbOfElements = nbOfElements; }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + Objects.hash(this.person);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        HbasePersons other = (HbasePersons) obj;
        return Objects.equals(this.person, other.person);
    }

    @Override
    public String toString() { return "HbasePersons [person=" + this.person + "]"; }

    /**
     * Creates builder to build {@link HbasePersons}.
     * 
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbasePersons}.
     */
    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private String person;
        private long   nbOfElements;

        private Builder() {}

        /**
         * Builder method for dataCreationDate parameter.
         * 
         * @param dataCreationDate
         *            field to set
         * @return builder
         */
        public Builder withDataCreationDate(long dataCreationDate) {
            this.dataCreationDate = dataCreationDate;
            return this;
        }

        /**
         * Builder method for dataId parameter.
         * 
         * @param dataId
         *            field to set
         * @return builder
         */
        public Builder withDataId(String dataId) {
            this.dataId = dataId;
            return this;
        }

        /**
         * Builder method for person parameter.
         * 
         * @param person
         *            field to set
         * @return builder
         */
        public Builder withPerson(String person) {
            this.person = person;
            return this;
        }

        /**
         * Builder method for nbOfElements parameter.
         * 
         * @param nbOfElements
         *            field to set
         * @return builder
         */
        public Builder withNbOfElements(long nbOfElements) {
            this.nbOfElements = nbOfElements;
            return this;
        }

        /**
         * Builder method of the builder.
         * 
         * @return built class
         */
        public HbasePersons build() { return new HbasePersons(this); }
    }

}
